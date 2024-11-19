from collections.abc import Callable
from typing import Any

import pandas as pd

class Builder:
    df = pd.DataFrame()

    def __init__(
        self,
        paths: list[str],
        depth: int,
        include_patterns: list[str],
        joblib_parallel_kwargs: dict[str, Any],
    ) -> None: ...
    def build(self, *, parsing_func: Callable[[str], pd.DataFrame]) -> Builder: ...
