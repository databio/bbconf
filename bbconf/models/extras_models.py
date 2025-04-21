import datetime
from typing import List, Optional, Union

from pydantic import BaseModel

from bbconf.models.base_models import FileModel


class ExtraFilesResults(BaseModel):
    limit: int = 0
    offset: int = 10
    total: int = 0
    results: List[FileModel]
