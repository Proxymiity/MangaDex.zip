import threading
import MangaDexPy
import functools
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
        rmtree(_task_path(task), ignore_errors=True)


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
    def __init__(self, data, light=False, language="en",
                 append_titles=False, preferred_groups=None, groups_substitute=True,
                 start=None, end=None):
        self.light = light
        self.language = language
        self.append_titles = append_titles
        self.preferred_groups = preferred_groups or []
        self.groups_substitute = groups_substitute
        self.start = start
        self.end = end
        super().__init__(data)

    def run(self, task):
        task.started = True
        task.status = f"Retrieving chapters for manga {self.data}"

        try:
            md = MangaDexPy.Client()
            md.session.request = functools.partial(md.session.request, timeout=10)
            md.session.headers["User-Agent"] = "Proxymiity/MangaDexZip"
            md.session.headers.pop("Authorization")
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
                "translatedLanguage[]": [self.language],
                "includeEmptyPages": 0,
                "includeFuturePublishAt": 0,
                "includeExternalUrl": 0
            })
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

        if self.preferred_groups:
            chaps = self.filter_groups(chaps)
        if self.start:
            chaps = self.filter_start(chaps)
        if self.end:
            chaps = self.filter_end(chaps)

        if not chaps:
            task.failed = True
            task.status = f"There are no chapters available for manga {manga.id} matching your filters"
            return

        dedup_none = False
        dedup_dict = {}
        for chap in chaps:
            try:
                chap_key = (chap.volume, float(chap.chapter))
            except (ValueError, TypeError):
                chap_key = (chap.volume, chap.chapter or 0)
                if chap.chapter is None:
                    dedup_none = True
            if chap_key not in dedup_dict:
                dedup_dict[chap_key] = chap

        if dedup_none and len(dedup_dict) > 1:
            for _, chap in sorted(dedup_dict.items(), key=lambda i: i[0][1]):
                task.add_action(DownloadChapter(chap.id, data_obj=chap, light=self.light, subfolder=True,
                                                append_title=self.append_titles,
                                                volume_dedupe=chap.chapter is None))
        else:
            for _, chap in sorted(dedup_dict.items(), key=lambda i: i[0][1]):
                task.add_action(DownloadChapter(chap.id, data_obj=chap, light=self.light, subfolder=True,
                                                append_title=self.append_titles))

        task.add_action(ArchiveContentsZIP())

    def filter_groups(self, chaps):
        filtered = []
        chaps_dict = {}
        for chap in chaps:
            try:
                chap_key = (chap.volume, float(chap.chapter))
            except (ValueError, TypeError):
                chap_key = (chap.volume, chap.chapter)
            if chap_key not in chaps_dict:
                chaps_dict[chap_key] = []
            chaps_dict[chap_key].append(chap)

        for chap_list in chaps_dict.values():
            chaps_groups = {chap: [c.id for c in chap.group] for chap in chap_list}
            for group in self.preferred_groups:
                for k, v in chaps_groups.items():
                    if group in v:
                        filtered.append(k)
                        break
                else:
                    continue
                break
            else:
                if self.groups_substitute:
                    filtered.append(tuple(chaps_groups.keys())[0])

        return filtered

    def filter_start(self, chaps):
        filtered = []
        for chap in chaps:
            try:
                if float(chap.chapter) >= self.start:
                    filtered.append(chap)
            except (ValueError, TypeError):
                continue
        return filtered

    def filter_end(self, chaps):
        filtered = []
        for chap in chaps:
            try:
                if float(chap.chapter) <= self.end:
                    filtered.append(chap)
            except (ValueError, TypeError):
                continue
        return filtered


class DownloadChapter(ActionBase):
    def __init__(self, data, data_obj=None, light=False, subfolder=False, append_title=False, volume_dedupe=False):
        self.data_obj = data_obj
        self.light = light
        self.subfolder = subfolder
        self.append_title = append_title
        self.volume_dedupe = volume_dedupe
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
                md.session.request = functools.partial(md.session.request, timeout=10)
                md.session.headers["User-Agent"] = "Proxymiity/MangaDexZip"
                md.session.headers.pop("Authorization")
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
            _chapter = f"Ch.{chap.chapter or '?'}"
            if self.volume_dedupe:
                _chapter = f"Ch.{chap.chapter or '?'} (Vol.{chap.volume or '?'})"
            if self.append_title and chap.title:
                p = Path(f"{_task_path_raw(task)}/{_chapter} - {str(chap.title)[:64]}")
            else:
                p = Path(f"{_task_path_raw(task)}/{_chapter}")
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
