from typing import Literal, Union

import pytest
from pydantic import Field

from dagster import job, op
from dagster._config.config_type import ConfigTypeKind
from dagster._config.structured_config import Config, PermissiveConfig
from dagster._core.errors import DagsterInvalidConfigError


def test_default_config_class_non_permissive():
    class AnOpConfig(Config):
        a_string: str
        an_int: int

    executed = {}

    @op
    def a_struct_config_op(config: AnOpConfig):
        executed["yes"] = True
        assert config.a_string == "foo"
        assert config.an_int == 2

    @job
    def a_job():
        a_struct_config_op()

    with pytest.raises(DagsterInvalidConfigError):
        a_job.execute_in_process(
            {
                "ops": {
                    "a_struct_config_op": {
                        "config": {"a_string": "foo", "an_int": 2, "a_bool": True}
                    }
                }
            }
        )


def test_struct_config_permissive():
    class AnOpConfig(PermissiveConfig):
        a_string: str
        an_int: int

    executed = {}

    @op
    def a_struct_config_op(config: AnOpConfig):
        executed["yes"] = True
        assert config.a_string == "foo"
        assert config.an_int == 2

        # Can pull out config dict to access permissive fields
        assert config.dict() == {"a_string": "foo", "an_int": 2, "a_bool": True}

    from dagster._core.definitions.decorators.solid_decorator import DecoratedOpFunction

    assert DecoratedOpFunction(a_struct_config_op).has_config_arg()

    # test fields are inferred correctly
    assert a_struct_config_op.config_schema.config_type.kind == ConfigTypeKind.PERMISSIVE_SHAPE
    assert list(a_struct_config_op.config_schema.config_type.fields.keys()) == [
        "a_string",
        "an_int",
    ]

    @job
    def a_job():
        a_struct_config_op()

    assert a_job

    a_job.execute_in_process(
        {
            "ops": {
                "a_struct_config_op": {"config": {"a_string": "foo", "an_int": 2, "a_bool": True}}
            }
        }
    )

    assert executed["yes"]


def test_descriminated_unions():
    class Cat(Config):
        pet_type: Literal["cat"]
        meows: int

    class Dog(Config):
        pet_type: Literal["dog"]
        barks: float

    class Lizard(Config):
        pet_type: Literal["reptile", "lizard"]
        scales: bool

    class OpConfigWithUnion(Config):
        pet: Union[Cat, Dog, Lizard] = Field(..., discriminator="pet_type")
        n: int

    executed = {}

    @op
    def a_struct_config_op(config: OpConfigWithUnion):

        if config.pet.pet_type == "cat":
            assert config.pet.meows == 2
        elif config.pet.pet_type == "dog":
            assert config.pet.barks == 3.0
        elif config.pet.pet_type == "lizard":
            assert config.pet.scales
        assert config.n == 4

        executed["yes"] = True

    @job
    def a_job():
        a_struct_config_op()

    assert a_job

    a_job.execute_in_process(
        {"ops": {"a_struct_config_op": {"config": {"pet": {"cat": {"meows": 2}}, "n": 4}}}}
    )
    assert executed["yes"]

    executed = {}
    a_job.execute_in_process(
        {"ops": {"a_struct_config_op": {"config": {"pet": {"dog": {"barks": 3.0}}, "n": 4}}}}
    )
    assert executed["yes"]

    executed = {}
    a_job.execute_in_process(
        {"ops": {"a_struct_config_op": {"config": {"pet": {"lizard": {"scales": True}}, "n": 4}}}}
    )
    assert executed["yes"]

    executed = {}
    a_job.execute_in_process(
        {"ops": {"a_struct_config_op": {"config": {"pet": {"reptile": {"scales": True}}, "n": 4}}}}
    )
    assert executed["yes"]

    # Ensure passing value which doesn't exist errors
    with pytest.raises(DagsterInvalidConfigError):
        a_job.execute_in_process(
            {"ops": {"a_struct_config_op": {"config": {"pet": {"octopus": {"meows": 2}}, "n": 4}}}}
        )

    # Disallow passing multiple discriminated union values
    with pytest.raises(DagsterInvalidConfigError):
        a_job.execute_in_process(
            {
                "ops": {
                    "a_struct_config_op": {
                        "config": {
                            "pet": {"reptile": {"scales": True}, "dog": {"barks": 3.0}},
                            "n": 4,
                        }
                    }
                }
            }
        )


def test_nested_discriminated_unions():
    class Poodle(Config):
        breed_type: Literal["poodle"]
        fluffy: bool

    class Dachshund(Config):
        breed_type: Literal["dachshund"]
        long: bool

    class Cat(Config):
        pet_type: Literal["cat"]
        meows: int

    class Dog(Config):
        pet_type: Literal["dog"]
        barks: float
        breed: Union[Poodle, Dachshund] = Field(..., discriminator="breed_type")

    class OpConfigWithUnion(Config):
        pet: Union[Cat, Dog] = Field(..., discriminator="pet_type")
        n: int

    executed = {}

    @op
    def a_struct_config_op(config: OpConfigWithUnion):

        assert config.pet.breed.fluffy  # type: ignore[union-attr]

        executed["yes"] = True

    @job
    def a_job():
        a_struct_config_op()

    assert a_job

    a_job.execute_in_process(
        {
            "ops": {
                "a_struct_config_op": {
                    "config": {
                        "pet": {"dog": {"barks": 3.0, "breed": {"poodle": {"fluffy": True}}}},
                        "n": 4,
                    }
                }
            }
        }
    )
    assert executed["yes"]
