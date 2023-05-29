import threading
import MangaDexPy
from requests import exceptions as rex

from datetime import datetime

from pathlib import Path
from shutil import rmtree
from os import listdir
from zipfile import ZipFile, ZIP_STORED

from time import sleep

from ..config import config

TEMP_PATH = config["backend"]["temp_path"]


def _task_path(task):
    return Path(f"{TEMP_PATH}/{task.uid}")


def _task_path_raw(task):
    return f"{TEMP_PATH}/{task.uid}"


class ActionBase:
    def __init__(self, data=None):
        self.data = data

    def run(self, task):
        pass


class DefaultCleanupAction(ActionBase):
    def run(self, task):
        rmtree(_task_path(task))


class ArchiveContentsZIP(ActionBase):
    def run(self, task):
        task.status = "Archiving contents"
        p = _task_path(task)
        zp = Path(f"{_task_path_raw(task)}/archive.zip")

        with ZipFile(zp, "w", compression=ZIP_STORED) as zf:
            self._archive_directory(task, p, zf, ignores=["archive.zip"])

        task.status = "Cleaning up..."
        self._cleanup_directory(p, ignores=["archive.zip"])

        task.status = "Task is ready for download"
        task.completed = True
        task.result = str(zp)

    def _archive_directory(self, task, path, arc, ignores=None, arc_path=""):
        ignores = ignores or []
        task.status = f"Archiving contents ({arc_path or '/'})"
        for o in listdir(path):
            p = Path(f"{path}/{o}")
            if p.name in ignores:
                continue
            elif p.is_dir():
                self._archive_directory(task, p, arc, arc_path=f"/{p.name}")
            elif p.is_file():
                arc.write(p, Path(f"{arc_path}/{p.name}"))

    @staticmethod
    def _cleanup_directory(path, ignores=None):
        ignores = ignores or []
        for o in listdir(path):
            p = Path(f"{path}/{o}")
            if p.name in ignores:
                continue
            elif p.is_dir():
                rmtree(p)
            elif p.is_file():
                p.unlink()


class AddMangaChapters(ActionBase):
    def __init__(self, data, light=False, language="en"):
        self.light = light
        self.language = language
        super().__init__(data)

    def run(self, task):
        task.started = True
        task.status = f"Retrieving chapters for manga {self.data}"

        try:
            md = MangaDexPy.Client()
            md.session.headers["User-Agent"] = "Proxymiity/MangaDexZip"
            manga = md.get_manga(self.data)
        except MangaDexPy.NoContentError:
            task.failed = True
            task.status = f"Manga {self.data} not found"
            return
        except MangaDexPy.APIError:
            task.failed = True
            task.status = f"MD API Error occurred during information fetch for manga {self.data}"
            return

        try:
            chaps = manga.get_chapters(params={
                "contentRating[]": ["safe", "suggestive", "erotica", "pornographic"],
                "translatedLanguage[]": [self.language]
            })
            # todo: advanced filtering
            if not chaps:
                task.failed = True
                task.status = f"There are no chapters available for manga {manga.id}"
                return
        except MangaDexPy.NoResultsError:
            task.failed = True
            task.status = f"There are no chapters available for manga {manga.id}"
            return
        except MangaDexPy.APIError:
            task.failed = True
            task.status = f"MD API Error occurred during chapters fetch for manga {manga.id}"
            return

        dedup_dict = {}
        for chap in chaps:
            if chap.chapter not in dedup_dict:
                try:
                    dedup_dict[float(chap.chapter)] = chap
                except (ValueError, TypeError):
                    dedup_dict[chap.chapter] = chap

        for _, chap in sorted(dedup_dict.items(), key=lambda i: i[0]):
            task.add_action(DownloadChapter(chap.id, data_obj=chap, light=self.light, subfolder=True))

        task.add_action(ArchiveContentsZIP())


class DownloadChapter(ActionBase):
    def __init__(self, data, data_obj=None, light=False, subfolder=False):
        self.data_obj = data_obj
        self.light = light
        self.subfolder = subfolder
        self.net = None
        super().__init__(data)

    def run(self, task):
        task.started = True
        task.status = f"Downloading chapter {self.data}"
        p = _task_path(task)
        p.mkdir(parents=True, exist_ok=True)

        t1 = datetime.now()
        if self.data_obj:
            chap = self.data_obj
        else:
            try:
                md = MangaDexPy.Client()
                md.session.headers["User-Agent"] = "Proxymiity/MangaDexZip"
                chap = md.get_chapter(self.data)
            except MangaDexPy.NoContentError:
                task.failed = True
                task.status = f"Chapter {self.data} Not Found"
                return
            except MangaDexPy.APIError:
                task.failed = True
                task.status = f"MD API Error occurred during information fetch for chapter {self.data}"
                return

        if self.subfolder:
            p = Path(f"{_task_path_raw(task)}/Ch.{chap.chapter or '?'}")
            p.mkdir(exist_ok=True)

        try:
            self.net = chap.get_md_network()
        except MangaDexPy.APIError:
            task.failed = True
            task.status = f"MD API Error occurred during server attribution for chapter {chap.id}"
            return

        threads = []
        pages = self.net.pages_redux if self.light else self.net.pages
        for x in pages:
            t = threading.Thread(target=self._page_dl, args=[chap, x, len(pages), p, task])
            threads.append(t)
        for y in threads:
            y.start()
        for i, y in enumerate(threads):
            task.status = f"Downloading " \
                          f"Vol.{chap.volume or '?'} Ch.{chap.chapter or '?'} p.{i}/{len(threads)}"
            y.join()

        t2 = datetime.now()
        rl_diff = 1.5 - (t2 - t1).total_seconds()
        if rl_diff > 0:
            sleep(rl_diff)

    def _page_dl(self, chapter, page, pages, path, task):
        i = 0
        while True:
            i += 1
            name = self.fmt_page(page.rsplit("/", 1)[1], pages)
            try:
                with self.net.client.session.get(page, timeout=5) as r:
                    with Path(f"{path}/{name}").open("wb") as f:
                        f.write(r.content)

                success = True if r.status_code < 400 else False
                try:
                    cached = True if r.headers["x-cache"] == "HIT" else False
                except KeyError:
                    cached = False

                try:
                    self.net.report(page, success, cached, len(r.content), int(r.elapsed.microseconds/1000))
                except MangaDexPy.APIError:
                    pass

            except rex.RequestException:
                if i == 5:
                    task.failed = True
                    task.status = f"MD Node Error when downloading page {name} from chapter {chapter.id}"
                    break
                task.status = f"MD Node Error when downloading page {name} from chapter {chapter.id}, retrying"
                sleep(1.5)
                self.net = chapter.get_md_network()

            else:
                break

    @staticmethod
    def fmt_page(page, length):
        num = ''.join([x for x in page.split("-")[0] if x.isdigit()])
        leading_zeros = len(str(length)) - len(num)
        final_name = ""
        for _ in range(leading_zeros):
            final_name += "0"
        final_name += num + Path(page).suffix
        return final_name
