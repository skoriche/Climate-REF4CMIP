from enum import Enum
from typing import Any, Optional

from pydantic import (
    BaseModel,
)


class OutputCV(Enum):
    """CMEC output bundle controlled vocabulary"""

    INDEX = "index"
    PROVENANCE = "provenance"
    DATA = "data"
    PLOTS = "plots"
    HTML = "html"
    METRICS = "metrics"
    FILENAME = "filename"
    LONG_NAME = "long_name"
    DESCRIPTION = "description"


class OutputIndex(BaseModel):
    """CMEC output bundle index object"""

    index: str = "index.html"


class OutputProvenance(BaseModel):
    """CMEC output bundle provenance object"""

    environment: dict[
        str, str
    ]  # Key/value pairs listing all relevant diagnostic and framework environment variables.
    modeldata: list[str, ...]  # type: ignore # Path to the model data used in this analysis.
    obsdata: (
        dict[
            str, Any
        ]  # 	Key/value pairs containing short names and versions of all observational datasets used.
    )
    log: str  # 	Filename of a free format log file written during execution.


class _OutputDict(BaseModel):
    filename: str  # Filename of plot produced (relative to output directory path)
    long_name: str  # Human readable name describing the plot
    description: str  # Description of what is depicted in the plot


class CMECOutput(BaseModel):
    """CMEC output bundle object"""

    index: str = "index.html"
    provenance: OutputProvenance
    data: Optional[dict[str, _OutputDict]] = None
    plots: Optional[dict[str, _OutputDict]] = None
    html: Optional[dict[str, _OutputDict]] = None
    metrics: Optional[dict[str, _OutputDict]] = None
