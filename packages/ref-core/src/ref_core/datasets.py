import enum


class SourceDatasetType(enum.Enum):
    """
    Types of supported source datasets
    """

    CMIP6 = "cmip6"
    CMIP7 = "cmip7"
