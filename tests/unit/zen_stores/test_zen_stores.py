#  Copyright (c) ZenML GmbH 2022. All Rights Reserved.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at:
#
#       https://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
#  or implied. See the License for the specific language governing
#  permissions and limitations under the License.
import os
import shutil
from pathlib import Path

import pytest

from zenml.constants import REPOSITORY_DIRECTORY_NAME
from zenml.enums import OrchestratorFlavor, StackComponentType, StoreType
from zenml.exceptions import StackComponentExistsError, StackExistsError
from zenml.orchestrators import LocalOrchestrator
from zenml.stack import Stack
from zenml.zen_stores import BaseZenStore, LocalZenStore, SqlZenStore
from zenml.zen_stores.models import StackComponentWrapper, StackWrapper


def _zen_store_for_type(store_type: StoreType, path: Path) -> BaseZenStore:
    if store_type == StoreType.LOCAL:
        return LocalZenStore().initialize(str(path))
    elif store_type == StoreType.SQL:
        return SqlZenStore().initialize(f"sqlite:///{path / 'store.db'}")
    else:
        raise NotImplementedError(f"No StackStore for {store_type}")


@pytest.mark.parametrize("store_type", [StoreType.LOCAL, StoreType.SQL])
def test_register_deregister_stacks(
    tmp_path_factory: pytest.TempPathFactory, store_type: StoreType
):
    """Test creating a new stack store."""
    tmp_path = tmp_path_factory.mktemp(f"{store_type.value}_stack_store")
    os.mkdir(tmp_path / REPOSITORY_DIRECTORY_NAME)

    stack = Stack.default_local_stack()

    # stack store is pre-initialized with the default stack
    zen_store = _zen_store_for_type(store_type, tmp_path)
    assert len(zen_store.stacks) == 1
    assert len(zen_store.stack_configurations) == 1

    # retrieve the default stack
    got_stack = zen_store.get_stack(stack.name)
    assert got_stack.name == stack.name
    stack_configuration = zen_store.get_stack_configuration(stack.name)
    assert set(stack_configuration) == {
        "orchestrator",
        "metadata_store",
        "artifact_store",
    }
    assert stack_configuration[StackComponentType.ORCHESTRATOR] == "default"

    # can't register the same stack twice or another stack with the same name
    with pytest.raises(StackExistsError):
        zen_store.register_stack(StackWrapper.from_stack(stack))
    with pytest.raises(StackExistsError):
        zen_store.register_stack(StackWrapper(name=stack.name, components=[]))

    # remove the default stack
    zen_store.deregister_stack(stack.name)
    assert len(zen_store.stacks) == 0
    with pytest.raises(KeyError):
        _ = zen_store.get_stack(stack.name)

    # now can add another stack with the same name
    zen_store.register_stack(StackWrapper(name=stack.name, components=[]))
    assert len(zen_store.stacks) == 1

    # clean up the temp fixture (TODO: how to parametrize a fixture?)
    shutil.rmtree(tmp_path)


@pytest.mark.parametrize("store_type", [StoreType.LOCAL, StoreType.SQL])
def test_register_deregister_components(
    tmp_path_factory: pytest.TempPathFactory, store_type: StoreType
):
    """Test adding and removing stack components."""
    tmp_path = tmp_path_factory.mktemp(f"{store_type.value}_stack_store")
    os.mkdir(tmp_path / REPOSITORY_DIRECTORY_NAME)

    required_components = {
        StackComponentType.ARTIFACT_STORE,
        StackComponentType.METADATA_STORE,
        StackComponentType.ORCHESTRATOR,
    }

    # stack store starts off with the default stack
    zen_store = _zen_store_for_type(store_type, tmp_path)
    for component_type in StackComponentType:
        component_type = StackComponentType(component_type)
        if component_type in required_components:
            assert len(zen_store.get_stack_components(component_type)) == 1
        else:
            assert len(zen_store.get_stack_components(component_type)) == 0

    # get a component
    orchestrator = zen_store.get_stack_component(
        StackComponentType.ORCHESTRATOR, "default"
    )
    assert orchestrator.flavor == OrchestratorFlavor.LOCAL
    assert orchestrator.name == "default"

    # can't add another orchestrator of same name
    with pytest.raises(StackComponentExistsError):
        zen_store.register_stack_component(
            StackComponentWrapper.from_component(
                LocalOrchestrator(
                    name="default",
                )
            )
        )

    # but can add one if it has a different name
    zen_store.register_stack_component(
        StackComponentWrapper.from_component(
            LocalOrchestrator(
                name="local_orchestrator_part_2_the_remix",
            )
        )
    )
    assert (
        len(zen_store.get_stack_components(StackComponentType.ORCHESTRATOR))
        == 2
    )

    # can't delete an orchestrator that's part of a stack
    with pytest.raises(ValueError):
        zen_store.deregister_stack_component(
            StackComponentType.ORCHESTRATOR, "default"
        )

    # but can if the stack is deleted first
    zen_store.deregister_stack("default")
    zen_store.deregister_stack_component(
        StackComponentType.ORCHESTRATOR, "default"
    )
    assert (
        len(zen_store.get_stack_components(StackComponentType.ORCHESTRATOR))
        == 1
    )
