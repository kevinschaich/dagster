import inspect
from typing import Generic, Mapping, TypeVar, Union

from typing_extensions import TypeAlias, dataclass_transform, get_origin

from dagster._config.config_type import ConfigType
from dagster._config.source import BoolSource, IntSource, StringSource
<<<<<<< HEAD:python_modules/dagster/dagster/_config/structured_config/__init__.py
from dagster._config.structured_config.typing_utils import AllowPartialResourceInitParams
from dagster._core.definitions.definition_config_schema import IDefinitionConfigSchema
=======
from dagster._core.definitions.definition_config_schema import (
    ConfiguredDefinitionConfigSchema,
    IDefinitionConfigSchema,
    convert_user_facing_definition_config_schema,
)
>>>>>>> 53472d8e86 ([structured config] Enable nesting of non-structured resources in structured resources):python_modules/dagster/dagster/_config/structured_config.py
from dagster._core.execution.context.init import InitResourceContext

try:
    from functools import cached_property
except ImportError:

    class cached_property:  # type: ignore[no-redef]
        pass


from abc import ABC, abstractmethod
from typing import AbstractSet, Any, Dict, Optional, Set, Tuple, Type, cast

from pydantic import BaseModel, Extra
from pydantic.fields import SHAPE_SINGLETON, ModelField

import dagster._check as check
from dagster import Field, Shape
from dagster._config.field_utils import (
    FIELD_NO_DEFAULT_PROVIDED,
    Permissive,
    config_dictionary_from_values,
    convert_potential_field,
)
from dagster._core.definitions.resource_definition import (
    ResourceDefinition,
    ResourceFunction,
    ResourceFunctionWithoutContext,
    is_context_provided,
)
from dagster._core.storage.io_manager import IOManager, IOManagerDefinition

from . import typing_utils
from .typing_utils import BaseResourceMeta

Self = TypeVar("Self", bound="Resource")


def _safe_is_subclass(cls: Any, possible_parent_cls: Type) -> bool:
    """Version of issubclass that returns False if cls is not a Type."""
    if not isinstance(cls, type):
        return False

    try:
        return issubclass(cls, possible_parent_cls)
    except TypeError:
        # Using builtin Python types in python 3.9+ will raise a TypeError when using issubclass
        # even though the isinstance check will succeed (as will inspect.isclass), for example
        # list[dict[str, str]] will raise a TypeError
        return False


class MakeConfigCacheable(BaseModel):
    """This class centralizes and implements all the chicanery we need in order
    to support caching decorators. If we decide this is a bad idea we can remove it
    all in one go.
    """

    # Pydantic config for this class
    # Cannot use kwargs for base class as this is not support for pydnatic<1.8
    class Config:
        # Various pydantic model config (https://docs.pydantic.dev/usage/model_config/)
        # Necessary to allow for caching decorators
        arbitrary_types_allowed = True
        # Avoid pydantic reading a cached property class as part of the schema
        keep_untouched = (cached_property,)
        # Ensure the class is serializable, for caching purposes
        frozen = True

    def __setattr__(self, name: str, value: Any):
        # This is a hack to allow us to set attributes on the class that are not part of the
        # config schema. Pydantic will normally raise an error if you try to set an attribute
        # that is not part of the schema.

        if name.startswith("_") or name.endswith("_cache"):
            object.__setattr__(self, name, value)
            return

        return super().__setattr__(name, value)


class Config(MakeConfigCacheable):
    """
    Base class for Dagster configuration models.
    """


class PermissiveConfig(Config):
    # Pydantic config for this class
    # Cannot use kwargs for base class as this is not support for pydantic<1.8
    class Config:
        extra = "allow"

    """
    Base class for Dagster configuration models that allow arbitrary extra fields.
    """


def _curry_config_schema(schema_field: Field, data: Any) -> IDefinitionConfigSchema:
    """Return a new config schema configured with the passed in data"""
    # We don't do anything with this resource definition, other than
    # use it to construct configured schema
    inner_resource_def = ResourceDefinition(lambda _: None, schema_field)
    configured_resource_def = inner_resource_def.configured(
        config_dictionary_from_values(
            data,
            schema_field,
        ),
    )
    # this cast required to make mypy happy, which does not support Self
    configured_resource_def = cast(ResourceDefinition, configured_resource_def)
    return configured_resource_def.config_schema


ResValue = TypeVar("ResValue")
IOManagerValue = TypeVar("IOManagerValue", bound=IOManager)


class AllowDelayedDependencies:
    _resource_pointers: Mapping[str, ResourceDefinition] = {}

    def _resolve_required_resource_keys(self) -> AbstractSet[str]:
        # All dependent resources which are not fully configured
        # must be specified to the Definitions object so that the
        # resource can be configured at runtime by the user
        pointer_keys = {k: v.top_level_key for k, v in self._resource_pointers.items()}
        check.invariant(
            all(pointer_key is not None for pointer_key in pointer_keys.values()),
            (
                "Any partially configured, nested resources must be specified to Definitions"
                f" object: {pointer_keys}"
            ),
        )

        # Recursively get all nested resource keys
        nested_pointer_keys: Set[str] = set()
        for v in self._resource_pointers.values():
            nested_pointer_keys.update(v.required_resource_keys)

        resources, _ = _separate_resource_params(self.__dict__)
        for v in resources.values():
            nested_pointer_keys.update(v.required_resource_keys)

        out = set(cast(Set[str], pointer_keys.values())).union(nested_pointer_keys)
        return out


class Resource(
    Generic[ResValue],
    ResourceDefinition,
    Config,
    AllowPartialResourceInitParams,
    AllowDelayedDependencies,
    metaclass=BaseResourceMeta,
):
    """
    Base class for Dagster resources that utilize structured config.

    This class is a subclass of both :py:class:`ResourceDefinition` and :py:class:`Config`, and
    provides a default implementation of the resource_fn that returns the resource itself.

    Example:
    .. code-block:: python

        class WriterResource(Resource):
            prefix: str

            def output(self, text: str) -> None:
                print(f"{self.prefix}{text}")

    """

    def __init__(self, **data: Any):
        resource_pointers, data_without_resources = _separate_resource_params(data)

        schema = infer_schema_from_config_class(
            self.__class__, ignore_resource_fields=set(resource_pointers.keys())
        )

        schema = _curry_config_schema(schema, data_without_resources)

        Config.__init__(self, **data)

        # We keep track of any resources we depend on which are not fully configured
        # so that we can retrieve them at runtime
        self._resource_pointers: Mapping[str, ResourceDefinition] = {
            k: v for k, v in resource_pointers.items() if (not _is_fully_configured(v))
        }

        ResourceDefinition.__init__(
            self,
            resource_fn=self.initialize_and_run,
            config_schema=schema,
            description=self.__doc__,
        )

    @classmethod
    def configure_at_launch(cls: "Type[Self]", **kwargs) -> "PartialResource[Self]":
        """
        Returns a partially initialized copy of the resource, with remaining config fields
        set at runtime.
        """
        return PartialResource(cls, data=kwargs)

    @property
    def required_resource_keys(self) -> AbstractSet[str]:
        return self._resolve_required_resource_keys()

    def initialize_and_run(self, context: InitResourceContext) -> ResValue:
        # If we have any partially configured resources, we need to update them
        # with the fully configured resources from the context

        _, config_to_update = _separate_resource_params(context.resource_config)

        partial_resources_to_update = {
            k: getattr(context.resources, cast(str, v.top_level_key))
            for k, v in self._resource_pointers.items()
        }

        resources_to_update, _ = _separate_resource_params(self.__dict__)
        resources_to_update = {
            k: _call_resource_fn_with_default(v, context)
            for k, v in resources_to_update.items()
            if k not in partial_resources_to_update
        }

        to_update = {**resources_to_update, **partial_resources_to_update, **config_to_update}

        for k, v in to_update.items():
            object.__setattr__(self, k, v)

        return self._create_object_fn(context)

    def _create_object_fn(self, context: InitResourceContext) -> ResValue:
        return self.create_object_to_pass_to_user_code(context)

    def create_object_to_pass_to_user_code(
        self, context: InitResourceContext
    ) -> ResValue:  # pylint: disable=unused-argument
        """
        Returns the object that this resource hands to user code, accessible by ops or assets
        through the context or resource parameters. This works like the function decorated
        with @resource when using function-based resources.

        Default behavior for new class-based resources is to return itself, passing
        the actual resource object to user code.
        """
        return cast(ResValue, self)


def _is_fully_configured(resource: ResourceDefinition) -> bool:
    return (
        ConfiguredDefinitionConfigSchema(
            resource, convert_user_facing_definition_config_schema(resource.config_schema), {}
        )
        .resolve_config({})
        .success
        is True
    )


class PartialResource(
    Generic[ResValue], ResourceDefinition, AllowDelayedDependencies, MakeConfigCacheable
):
    data: Dict[str, Any]
    resource_cls: Type[Resource[ResValue]]

    def __init__(self, resource_cls: Type[Resource[ResValue]], data: Dict[str, Any]):
        resource_pointers, data_without_resources = _separate_resource_params(data)

        MakeConfigCacheable.__init__(self, data=data, resource_cls=resource_cls)

        # We keep track of any resources we depend on which are not fully configured
        # so that we can retrieve them at runtime
        self._resource_pointers: Dict[str, ResourceDefinition] = {
            k: v for k, v in resource_pointers.items() if (not _is_fully_configured(v))
        }

        schema = infer_schema_from_config_class(
            resource_cls, ignore_resource_fields=set(resource_pointers.keys())
        )

        def resource_fn(context: InitResourceContext):
            instantiated = resource_cls(**context.resource_config, **data)
            return instantiated.initialize_and_run(context)

        ResourceDefinition.__init__(
            self,
            resource_fn=resource_fn,
            config_schema=schema,
            description=resource_cls.__doc__,
        )

    @property
    def required_resource_keys(self) -> AbstractSet[str]:
        return self._resolve_required_resource_keys()


ResourceOrPartial: TypeAlias = Union[Resource[ResValue], PartialResource[ResValue]]
ResourceOrPartialOrBase: TypeAlias = Union[
    Resource[ResValue], PartialResource[ResValue], ResourceDefinition, ResValue
]


V = TypeVar("V")


class ResourceDependency(Generic[V]):
    def __set_name__(self, _owner, name):
        self._name = name

    def __get__(self, obj: "Resource", __owner: Any) -> V:
        return getattr(obj, self._name)

    def __set__(self, obj: Optional[object], value: ResourceOrPartialOrBase[V]) -> None:
        setattr(obj, self._name, value)


class StructuredResourceAdapter(Resource, ABC):
    """
    Adapter base class for wrapping a decorated, function-style resource
    with structured config.

    To use this class, subclass it, define config schema fields using Pydantic,
    and implement the ``wrapped_resource`` method.

    Example:
    .. code-block:: python

        @resource(config_schema={"prefix": str})
        def writer_resource(context):
            prefix = context.resource_config["prefix"]

            def output(text: str) -> None:
                out_txt.append(f"{prefix}{text}")

            return output

        class WriterResource(StructuredResourceAdapter):
            prefix: str

            @property
            def wrapped_resource(self) -> ResourceDefinition:
                return writer_resource
    """

    @property
    @abstractmethod
    def wrapped_resource(self) -> ResourceDefinition:
        raise NotImplementedError()

    @property
    def resource_fn(self) -> ResourceFunction:
        return self.wrapped_resource.resource_fn

    def __call__(self, *args, **kwargs):
        return self.wrapped_resource(*args, **kwargs)


class StructuredConfigIOManagerBase(Resource[IOManagerValue], IOManagerDefinition):
    """
    Base class for Dagster IO managers that utilize structured config. This base class
    is useful for cases in which the returned IO manager is not the same as the class itself
    (e.g. when it is a wrapper around the actual IO manager implementation).

    This class is a subclass of both :py:class:`IOManagerDefinition` and :py:class:`Config`.
    Implementers should provide an implementation of the :py:meth:`resource_function` method,
    which should return an instance of :py:class:`IOManager`.
    """

    def __init__(self, **data: Any):
        Resource.__init__(self, **data)
        IOManagerDefinition.__init__(
            self,
            resource_fn=self.initialize_and_run,
            config_schema=self._config_schema,
            description=self.__doc__,
        )

    def _create_object_fn(self, context: InitResourceContext) -> IOManagerValue:
        return self.create_io_manager_to_pass_to_user_code(context)

    @abstractmethod
    def create_io_manager_to_pass_to_user_code(self, context) -> IOManagerValue:
        """Implement as one would implement a @io_manager decorator function"""
        raise NotImplementedError()


class StructuredConfigIOManager(StructuredConfigIOManagerBase, IOManager):
    """
    Base class for Dagster IO managers that utilize structured config.

    This class is a subclass of both :py:class:`IOManagerDefinition`, :py:class:`Config`,
    and :py:class:`IOManager`. Implementers must provide an implementation of the
    :py:meth:`handle_output` and :py:meth:`load_input` methods.
    """

    def create_io_manager_to_pass_to_user_code(self, context) -> IOManager:
        return self


def _convert_pydantic_field(pydantic_field: ModelField) -> Field:
    """
    Transforms a Pydantic field into a corresponding Dagster config field.
    """
    if _safe_is_subclass(pydantic_field.type_, Config):
        return infer_schema_from_config_class(
            pydantic_field.type_, description=pydantic_field.field_info.description
        )

    if pydantic_field.shape != SHAPE_SINGLETON:
        raise NotImplementedError(f"Pydantic shape {pydantic_field.shape} not supported")

    return Field(
        config=_config_type_for_pydantic_field(pydantic_field),
        description=pydantic_field.field_info.description,
        is_required=_is_pydantic_field_required(pydantic_field),
        default_value=pydantic_field.default
        if pydantic_field.default
        else FIELD_NO_DEFAULT_PROVIDED,
    )


def _config_type_for_pydantic_field(pydantic_field: ModelField) -> ConfigType:
    return _config_type_for_type_on_pydantic_field(pydantic_field.type_)


def _config_type_for_type_on_pydantic_field(potential_dagster_type: Any) -> ConfigType:
    # special case raw python literals to their source equivalents
    if potential_dagster_type is str:
        return StringSource
    elif potential_dagster_type is int:
        return IntSource
    elif potential_dagster_type is bool:
        return BoolSource
    else:
        return convert_potential_field(potential_dagster_type).config_type


def _is_pydantic_field_required(pydantic_field: ModelField) -> bool:
    # required is of type BoolUndefined = Union[bool, UndefinedType] in Pydantic
    if isinstance(pydantic_field.required, bool):
        return pydantic_field.required

    raise Exception(
        "pydantic.field.required is their UndefinedType sentinel value which we "
        "do not fully understand the semantics of right now. For the time being going "
        "to throw an error to figure see when we actually encounter this state."
    )


class StructuredIOManagerAdapter(StructuredConfigIOManagerBase):
    @property
    @abstractmethod
    def wrapped_io_manager(self) -> IOManagerDefinition:
        raise NotImplementedError()

    def create_io_manager_to_pass_to_user_code(self, context) -> IOManager:
        raise NotImplementedError(
            "Because we override resource_fn in the adapter, this is never called."
        )

    @property
    def resource_fn(self) -> ResourceFunction:
        return self.wrapped_io_manager.resource_fn


def infer_schema_from_config_annotation(model_cls: Any, config_arg_default: Any) -> Field:
    """
    Parses a structured config class or primitive type and returns a corresponding Dagster config Field.
    """
    if _safe_is_subclass(model_cls, Config):
        check.invariant(
            config_arg_default is inspect.Parameter.empty,
            "Cannot provide a default value when using a Config class",
        )
        return infer_schema_from_config_class(model_cls)

    # If were are here config is annotated with a primitive type
    # We do a conversion to a type as if it were a type on a pydantic field
    inner_config_type = _config_type_for_type_on_pydantic_field(model_cls)
    return Field(
        config=inner_config_type,
        default_value=FIELD_NO_DEFAULT_PROVIDED
        if config_arg_default is inspect.Parameter.empty
        else config_arg_default,
    )


def infer_schema_from_config_class(
    model_cls: Type[Config],
    description: Optional[str] = None,
    ignore_resource_fields: Optional[Set[str]] = None,
) -> Field:
    """
    Parses a structured config class and returns a corresponding Dagster config Field.
    """
    ignore_resource_fields = ignore_resource_fields or set()

    check.param_invariant(
        issubclass(model_cls, Config),
        "Config type annotation must inherit from dagster._config.structured_config.Config",
    )

    fields = {}
    for pydantic_field in model_cls.__fields__.values():
        if pydantic_field.name not in ignore_resource_fields:
            fields[pydantic_field.alias] = _convert_pydantic_field(pydantic_field)

    shape_cls = Permissive if model_cls.__config__.extra == Extra.allow else Shape

    docstring = model_cls.__doc__.strip() if model_cls.__doc__ else None
    return Field(config=shape_cls(fields), description=description or docstring)


def _separate_resource_params(
    data: Dict[str, Any]
) -> Tuple[Dict[str, Union[Resource, PartialResource, ResourceDefinition]], Dict[str, Any]]:
    """
    Separates out the key/value inputs of fields in a structured config Resource class which
    are themselves Resources and those which are not.
    """
    return (
        {
            k: v
            for k, v in data.items()
            if isinstance(v, (Resource, PartialResource, ResourceDefinition))
        },
        {
            k: v
            for k, v in data.items()
            if not isinstance(v, (Resource, PartialResource, ResourceDefinition))
        },
    )


def _call_resource_fn_with_default(obj: ResourceDefinition, context: InitResourceContext) -> Any:
    if isinstance(obj.config_schema, ConfiguredDefinitionConfigSchema):
        value = cast(Dict[str, Any], obj.config_schema.resolve_config({}).value)
        context = context.replace_config(value["config"])
    elif obj.config_schema.default_provided:
        context = context.replace_config(obj.config_schema.default_value)
    if is_context_provided(obj.resource_fn):
        return obj.resource_fn(context)
    else:
        return cast(ResourceFunctionWithoutContext, obj.resource_fn)()


typing_utils._Resource = Resource
typing_utils._PartialResource = PartialResource
typing_utils._ResourceDep = ResourceDependency