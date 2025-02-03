import pathlib
from collections import Counter
from enum import StrEnum
from typing import Any, Optional, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    FilePath,
    RootModel,
    ValidationError,
    ValidationInfo,
    model_validator,
    validate_call,
)
from pydantic.json_schema import GenerateJsonSchema, JsonSchemaValue


class MetricCV(StrEnum):
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
    name: str
    version: str
    package: str


class MetricDimensions(RootModel):
    """
    CMEC metric bundle DIMENSIONS object
    """

    root: dict[str, list | dict[str, Any]] = Field(
        default={
            MetricCV.JSON_STRUCTURE.value: ["model", "metric"],
            "model": {},
            "metric": {},
        }
    )

    @model_validator(mode="after")
    def validate_dimensions(self) -> Self:
        # assert the items in json_structure are same as the keys of dimensions

        assert MetricCV.JSON_STRUCTURE.value in self.root.keys(), "Error: json_strucuture is required keyword"

        assert Counter(self.root[MetricCV.JSON_STRUCTURE.value]) == Counter(
            [k for k in self.root.keys() if k != MetricCV.JSON_STRUCTURE.value]
        ), "Error: json_structure items are not in the keys of the DIMENSIONS"

        return self

    @validate_call
    def add_dimension(self, dim_name: str, dim_content: dict[str, Any]):
        """
        Add or update one dimension to MetricDimensions object
        """
        if dim_name in self.root[MetricCV.JSON_STRUCTURE.value]:
            self.root[dim_name].update(dim_content)

        else:
            self.root[MetricCV.JSON_STRUCTURE.value].append(dim_name)
            self.root[dim_name] = dim_content

    @classmethod
    def merge_dimension(cls, metric_dim1, metric_dim2):
        mdim1 = cls.model_validate(metric_dim1)
        mdim2 = cls.model_validate(metric_dim2)

        assert mdim1.root[MetricCV.JSON_STRUCTURE.value] == mdim2.root[MetricCV.JSON_STRUCTURE.value], (
            "Error: JSON_STRUCTURES are not same"
        )

        merged_dim = {}
        merged_dim = {MetricCV.JSON_STRUCTURE.value: mdim1.root[MetricCV.JSON_STRUCTURE.value]}

        for dim in mdim1.root[MetricCV.JSON_STRUCTURE.value]:
            merged_dim[dim] = mdim1.root[dim]

        for dim in mdim2.root[MetricCV.JSON_STRUCTURE.value]:
            for key in mdim2.root[dim].keys():
                if key not in merged_dim[dim].keys():
                    merged_dim[dim][key] = mdim2.root[dim][key]
        return cls(merged_dim)


class MetricResults(RootModel):
    """
    CMEC metric bundle RESULTS object
    """

    model_config = ConfigDict(strict=True)
    root: dict[str, dict]

    def __getitem__(self, key: str):
        return self.root[key]

    def __setitem__(self, key: str, value: dict):
        self.root[key] = value

    @classmethod
    def _check_nested_dict_keys(cls, nested, metdims, level=0, keylevs=None):
        if keylevs is None:
            keylevs = {}

        dim_name = metdims[MetricCV.JSON_STRUCTURE.value][level]

        assert Counter(list(metdims[dim_name].keys())) == Counter(nested.keys()), "Error in dict"

        for key, value in nested.items():
            if level not in keylevs:
                keylevs[level] = []
            keylevs[level].append(key)
            if isinstance(value, dict) and level < len(metdims[MetricCV.JSON_STRUCTURE.value]) - 1:
                cls._check_nested_dict_keys(value, metdims, level + 1, keylevs)
            elif isinstance(value, dict):
                StrNumDict(value)

        return keylevs

    @model_validator(mode="after")
    @classmethod
    def validate_results(cls, rlt, info: ValidationInfo) -> dict:
        if not isinstance(info.context, MetricDimensions):
            print(
                """To validate MetricResults object, MetricDimensions is needed,
            please use model_validate(Results, context=MetricDimensions to instantiate"""
            )
            raise ValidationError
        else:
            results = rlt.root
            metdims = info.context.root
            keylevs = cls._check_nested_dict_keys(results, metdims, level=0, keylevs=None)

        return rlt


class StrNumDict(RootModel):
    model_config = ConfigDict(strict=True)
    root: dict[str, float]


class CMECMetric(BaseModel):
    """
    CMEC metric bundle object
    """

    model_config = ConfigDict(strict=True)

    SCHEMA: Optional[MetricSchema] = None
    DIMENSIONS: MetricDimensions
    RESULTS: dict
    PROVENANCE: Optional[dict] = None
    DISCLAIMER: Optional[dict] = None
    NOTES: Optional[dict] = None

    @model_validator(mode="after")
    def validate_results(self) -> Self:
        if self.DIMENSIONS and self.RESULTS:
            # validate results data
            results = self.RESULTS
            MetricResults.model_validate(results, context=self.DIMENSIONS)
        return self

    @validate_call
    def dump_to_json(self, json_path: str = "./cmec.json"):
        pathlib.Path(json_path).write_text(self.model_dump_json(indent=2))

    @classmethod
    @validate_call
    def load_from_json(cls, jsonfile: FilePath):
        """
        Create CMECMetric object from a compatiable json file
        """
        json_str = pathlib.Path(jsonfile).read_text()

        metric_obj = cls.model_validate_json(json_str)

        return metric_obj

    @classmethod
    def _merge(cls, dict1: dict, dict2: dict):
        for key in dict2:
            if key in dict1:
                if isinstance(dict1[key], dict) and isinstance(dict2[key], dict):
                    cls._merge(dict1[key], dict2[key])

            else:
                dict1[key] = dict2[key]
        return dict1

    @classmethod
    def _fill(cls, mdict: dict, mdims: dict, level=0):
        dim_name = mdims[MetricCV.JSON_STRUCTURE.value][level]
        for key in mdims[dim_name].keys():
            if key not in mdict:
                mdict[key] = {}

        for key, value in mdict.items():
            if isinstance(value, dict) and level < len(mdims[MetricCV.JSON_STRUCTURE.value]) - 1:
                cls._fill(value, mdims, level + 1)

    @classmethod
    @validate_call
    def merge(cls, metric_obj1, metric_obj2, nodata: float):
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

    def generate(self, schema, mode="validation") -> JsonSchemaValue:
        json_schema = super().generate(schema, mode=mode)
        json_schema["title"] = "CMEC"
        json_schema["$schema"] = self.schema_dialect
        return json_schema
