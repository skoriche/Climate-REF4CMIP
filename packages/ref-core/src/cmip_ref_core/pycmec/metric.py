"""
CMEC metric bundle class
"""

import pathlib
from collections import Counter
from enum import Enum
from typing import Any, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    RootModel,
    ValidationInfo,
    field_validator,
    model_validator,
    validate_call,
)
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaMode, JsonSchemaValue
from pydantic_core import CoreSchema
from typing_extensions import Self


class MetricCV(Enum):
    """
    CMEC metric bundle controlled vocabulary
    """

    DIMENSIONS = "DIMENSIONS"
    JSON_STRUCTURE = "json_structure"
    RESULTS = "RESULTS"
    PROVENANCE = "PROVENANCE"
    DISCLAIMER = "DISCLAMER"
    NOTES = "NOTES"


class MetricSchema(BaseModel):
    """
    A metric schema used by unified dasbboard, not required by CMEC
    """

    name: str
    version: str
    package: str


# class MetricDimensions(RootModel[dict[str, Union[list[str], dict[str, Any]]]]):
class MetricDimensions(RootModel[Any]):
    """
    CMEC metric bundle DIMENSIONS object
    """

    root: dict[str, Any] = Field(
        # root: TypedDict = Field(
        default={
            MetricCV.JSON_STRUCTURE.value: ["model", "metric"],
            "model": {},
            "metric": {},
        }
    )

    @model_validator(mode="after")
    def validate_dimensions(self) -> Self:
        """Validate a MetricDimensions object"""
        # assert the items in json_structure are same as the keys of dimensions

        if MetricCV.JSON_STRUCTURE.value not in self.root.keys():
            raise ValueError("json_strucuture is required keyword")

        if not (
            Counter(self.root[MetricCV.JSON_STRUCTURE.value])
            == Counter([k for k in self.root.keys() if k != MetricCV.JSON_STRUCTURE.value])
        ):
            raise ValueError("json_structure items are not in the keys of the DIMENSIONS")

        return self

    @validate_call
    def add_dimension(self, dim_name: str, dim_content: dict[str, Any]) -> None:
        """
        Add or update one dimension to MetricDimensions object
        """
        if dim_name in self.root[MetricCV.JSON_STRUCTURE.value]:
            self.root[dim_name].update(dim_content)

        else:
            self.root[MetricCV.JSON_STRUCTURE.value].append(dim_name)
            self.root[dim_name] = dim_content

    @classmethod
    def merge_dimension(cls, metric_dim1: Any, metric_dim2: Any) -> Self:
        """Merge two MetricDimensions objects"""
        mdim1 = cls.model_validate(metric_dim1)
        mdim2 = cls.model_validate(metric_dim2)

        if not (mdim1.root[MetricCV.JSON_STRUCTURE.value] == mdim2.root[MetricCV.JSON_STRUCTURE.value]):
            raise ValueError("JSON_STRUCTURES are not same")

        merged_dim = {}
        merged_dim = {MetricCV.JSON_STRUCTURE.value: mdim1.root[MetricCV.JSON_STRUCTURE.value]}

        for dim in mdim1.root[MetricCV.JSON_STRUCTURE.value]:
            merged_dim[dim] = mdim1.root[dim]

        for dim in mdim2.root[MetricCV.JSON_STRUCTURE.value]:
            for key in mdim2.root[dim].keys():
                if key not in merged_dim[dim].keys():
                    merged_dim[dim][key] = mdim2.root[dim][key]
        return cls(merged_dim)


class MetricResults(RootModel[Any]):
    """
    CMEC metric bundle RESULTS object
    """

    model_config = ConfigDict(strict=True)
    root: dict[str, dict[Any, Any]]

    @classmethod
    def _check_nested_dict_keys(cls, nested: dict[Any, Any], metdims: dict[Any, Any], level: int = 0) -> None:
        dim_name = metdims[MetricCV.JSON_STRUCTURE.value][level]

        if not (Counter(list(metdims[dim_name].keys())) == Counter(nested.keys())):
            raise ValueError("Error in dicts of Results")

        for key, value in nested.items():
            if isinstance(value, dict) and level < len(metdims[MetricCV.JSON_STRUCTURE.value]) - 1:
                cls._check_nested_dict_keys(value, metdims, level + 1)
            elif isinstance(value, dict):
                StrNumDict(value)

    @field_validator("root", mode="after")
    @classmethod
    def validate_results(cls, rlt: Any, info: ValidationInfo) -> Any:
        """Validate a MeticResults object"""
        if not isinstance(info.context, MetricDimensions):
            s = "\nTo validate MetricResults object, MetricDimensions is needed,\n"
            s += "please use model_validate(Results, context=MetricDimensions to instantiate\n"
            raise ValueError(s)
        else:
            # results = rlt.root
            results = rlt
            metdims = info.context.root
            cls._check_nested_dict_keys(results, metdims, level=0)

        return rlt


class StrNumDict(RootModel[Any]):
    """A class contains string key and numeric value"""

    model_config = ConfigDict(strict=True)
    root: dict[str, float]


class CMECMetric(BaseModel):
    """
    CMEC metric bundle object
    """

    model_config = ConfigDict(strict=True)

    SCHEMA: Optional[MetricSchema] = None
    DIMENSIONS: MetricDimensions
    RESULTS: dict[str, Any]
    PROVENANCE: Optional[dict[str, Any]] = None
    DISCLAIMER: Optional[dict[str, Any]] = None
    NOTES: Optional[dict[str, Any]] = None

    @model_validator(mode="after")
    def validate_results(self) -> Self:
        """Validate a CMECMetric object"""
        # validate results data
        results = self.RESULTS
        MetricResults.model_validate(results, context=self.DIMENSIONS)
        return self

    @validate_call
    def dump_to_json(self, json_path: str = "./cmec.json") -> None:
        """Save the CMECMetric object to a file in JSON format"""
        pathlib.Path(json_path).write_text(self.model_dump_json(indent=2))

    @classmethod
    @validate_call
    def load_from_json(cls, jsonfile: FilePath) -> Self:
        """
        Create CMECMetric object from a compatiable json file
        """
        json_str = pathlib.Path(jsonfile).read_text()
        metric_obj = cls.model_validate_json(json_str)

        return metric_obj

    @classmethod
    def _merge(cls, dict1: dict[Any, Any], dict2: dict[Any, Any]) -> dict[Any, Any]:
        for key in dict2:
            if key in dict1:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    cls._merge(dict1[key], dict2[key])

            else:
                dict1[key] = dict2[key]
        return dict1

    @classmethod
    def _fill(cls, mdict: dict[Any, Any], mdims: dict[Any, Any], level: int = 0) -> None:
        dim_name = mdims[MetricCV.JSON_STRUCTURE.value][level]
        for key in mdims[dim_name].keys():
            if key not in mdict:
                mdict[key] = {}

        for key, value in mdict.items():
            if isinstance(value, dict) and level < len(mdims[MetricCV.JSON_STRUCTURE.value]) - 1:
                cls._fill(value, mdims, level + 1)

    @classmethod
    @validate_call
    def merge(cls, metric_obj1: Any, metric_obj2: Any, nodata: float) -> Self:
        """Merge two CMECMetric objects"""
        mobj1 = cls.model_validate(metric_obj1)
        mobj2 = cls.model_validate(metric_obj2)

        merged_obj_dims = MetricDimensions.merge_dimension(mobj1.DIMENSIONS, mobj2.DIMENSIONS)

        merged_obj_rlts = {}

        result1 = mobj2.RESULTS
        result2 = mobj1.RESULTS
        merged_obj_rlts = cls._merge(dict(result1), result2)

        cls._fill(merged_obj_rlts, merged_obj_dims.root)

        MetricResults.model_validate(merged_obj_rlts, context=merged_obj_dims)

        return cls(DIMENSIONS=merged_obj_dims, RESULTS=merged_obj_rlts)


class CMECGenerateJsonSchema(GenerateJsonSchema):
    """
    Customized CMEC JSON schema generation
    """

    def generate(self: Self, schema: CoreSchema, mode: JsonSchemaMode = "validation") -> JsonSchemaValue:
        """Generate customized json schema"""
        json_schema = super().generate(schema, mode=mode)
        json_schema["title"] = "CMEC"
        json_schema["$schema"] = self.schema_dialect
        return json_schema
