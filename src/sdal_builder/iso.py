# src/sdal_builder/iso.py
import pycdlib
from datetime import datetime
import pathlib
import os
from tqdm import tqdm

class TqdmFileWrapper:
    """
    Обертка для файла, которая обновляет tqdm при записи.
    Гарантирует, что бар дойдет до 100% перед закрытием.
    """
    def __init__(self, path, total_size, desc="Writing ISO"):
        self.path = path
        self.total_size = total_size
        self.fp = open(path, "wb")
        self.pbar = tqdm(total=total_size, unit="B", unit_scale=True, desc=desc, leave=True)

    def write(self, data):
        # pycdlib пишет байты
        n = len(data)
        self.fp.write(data)
        self.pbar.update(n)
        return n

    def close(self):
        # Принудительно добиваем бар до 100%, если pycdlib ошибся в расчетах (бывает из-за паддинга)
        if self.pbar.n < self.pbar.total:
            self.pbar.update(self.pbar.total - self.pbar.n)
        
        self.pbar.close()
        self.fp.close()
        
    def tell(self):
        return self.fp.tell()
        
    def seek(self, offset, whence=0):
        return self.fp.seek(offset, whence)


def build_iso(sdl_files: list[pathlib.Path], out_iso: pathlib.Path) -> None:
    """
    Создает ISO образ с честным прогресс-баром.
    """
    iso = pycdlib.PyCdlib()
    
    # Генерируем ID тома (YYMMDD_HH)
    volid = datetime.now().strftime("%y%m%d_%H")
    iso.new(vol_ident=volid, interchange_level=3)

    # Добавляем файлы в структуру ISO (виртуально)
    total_size = 0
    for fpath in sdl_files:
        name = fpath.name.upper()
        iso.add_file(str(fpath), f"/{name};1")
        total_size += fpath.stat().st_size

    # Добавляем примерный оверхед файловой системы ISO (~2MB) для красоты бара
    estimated_iso_size = total_size + (2 * 1024 * 1024) 

    # Пишем на диск через обертку с баром
    wrapper = TqdmFileWrapper(out_iso, estimated_iso_size, desc=f"Writing ISO ({out_iso.name})")
    try:
        iso.write_fp(wrapper)
    finally:
        wrapper.close()
        iso.close()