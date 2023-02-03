from typing import TYPE_CHECKING, Any, Generic, Optional, Type, TypeVar, Union, cast

import pydantic
from typing_extensions import TypeVar, dataclass_transform, get_args, get_origin

from .utils import safe_is_subclass

if TYPE_CHECKING:
    from dagster._config.structured_config import PartialResource

Self = TypeVar("Self", bound="AllowPartialResourceInitParams")


# Since a metaclass is invoked by Resource before Resource or PartialResource is defined, we need to
# define a temporary class to use as a placeholder for use in the initial metaclass invocation.
# When the metaclass is invoked for a Resource subclass, it will use the non-placeholder values.

_ResValue = TypeVar("_ResValue")


# Since a metaclass is invoked by Resource before Resource or PartialResource is defined, we need to
# define a temporary class to use as a placeholder for use in the initial metaclass invocation.
# When the metaclass is invoked for a Resource subclass, it will use the non-placeholder values.
class _Temp(Generic[_ResValue]):
    pass


_ResourceDep: Type = _Temp
_Resource: Type = _Temp
_PartialResource: Type = _Temp


@dataclass_transform()
class BaseResourceMeta(pydantic.main.ModelMetaclass):
    """
    Custom metaclass for Resource and PartialResource. This metaclass is responsible for
    transforming the type annotations on the class to allow for partially configured resources.
    """

    def __new__(self, name, bases, namespaces, **kwargs):
        # Gather all type annotations from the class and its base classes
        annotations = namespaces.get("__annotations__", {})
        for base in bases:
            if hasattr(base, "__annotations__"):
                annotations.update(base.__annotations__)
        for field in annotations:
            if not field.startswith("__"):
                # Check if the annotation is a ResourceDependency
                if get_origin(annotations[field]) == _ResourceDep:
                    # arg = get_args(annotations[field])[0]
                    # If so, we treat it as a Union of a PartialResource and a Resource
                    # for Pydantic's sake.
                    annotations[field] = Any
                elif safe_is_subclass(annotations[field], _Resource):
                    # If the annotation is a Resource, we treat it as a Union of a PartialResource
                    # and a Resource for Pydantic's sake, so that a user can pass in a partially
                    # configured resource.
                    base = annotations[field]
                    annotations[field] = Union[_PartialResource[base], base]

        namespaces["__annotations__"] = annotations
        return super().__new__(self, name, bases, namespaces, **kwargs)


class AllowPartialResourceInitParams:
    """
    Implementation of the Python descriptor protocol (https://docs.python.org/3/howto/descriptor.html)
    to adjust the types of resource inputs and outputs, e.g. resource dependencies can be passed in
    as PartialResources or Resources, but will always be returned as Resources.

    For example, given a resource with the following signature:

    .. code-block:: python

        class FooResource(Resource):
            bar: BarResource

    The following code will work:

    .. code-block:: python

        # Types as PartialResource[BarResource]
        partial_bar = BarResource.configure_at_runtime()

        # bar parameter takes BarResource | PartialResource[BarResource]
        foo = FooResource(bar=partial_bar)

        # bar attribute is BarResource
        print(foo.bar)

    Very similar to https://github.com/pydantic/pydantic/discussions/4262.
    """

    def __set_name__(self, _owner, name):
        self._assigned_name = name

    def __get__(self: "Self", obj: Any, __owner: Any) -> "Self":
        # no-op implementation (only used to affect type signature)
        return cast(Self, getattr(obj, self._assigned_name))

    def __set__(
        self: "Self", obj: Optional[object], value: Union["Self", "PartialResource[Self]"]
    ) -> None:
        # no-op implementation (only used to affect type signature)
        setattr(obj, self._assigned_name, value)
