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
import base64
import json
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, TypeVar, Union, overload
from uuid import UUID

from zenml.enums import StackComponentType, StoreType
from zenml.exceptions import StackComponentExistsError
from zenml.io import fileio
from zenml.io.utils import (
    read_file_contents_as_string,
    write_file_contents_as_string,
)
from zenml.logger import get_logger
from zenml.stack_stores import BaseStackStore
from zenml.stack_stores.models import (
    Project,
    Role,
    RoleAssignment,
    StackComponentWrapper,
    StackStoreModel,
    Team,
    User,
)
from zenml.utils import yaml_utils

logger = get_logger(__name__)

E = TypeVar("E", bound=Union[User, Team, Project, Role])


@overload
def _get_unique_entity(
    entity_name: str, collection: List[E], ensure_exists: bool = True
) -> E:
    ...


@overload
def _get_unique_entity(
    entity_name: str, collection: List[E], ensure_exists: bool = False
) -> Optional[E]:
    ...


def _get_unique_entity(
    entity_name: str, collection: List[E], ensure_exists: bool = True
) -> Optional[E]:
    matches = [entity for entity in collection if entity.name == entity_name]
    if len(matches) > 1:
        # Two entities with the same name, this should never happen
        raise RuntimeError(
            f"Found two or more entities with name '{entity_name}' of type "
            f"`{type(matches[0])}`."
        )

    if ensure_exists:
        if not matches:
            raise RuntimeError(f"No entity found with name '{entity_name}'.")
        return matches[0]
    else:
        return matches[0] if matches else None


class LocalStackStore(BaseStackStore):
    def initialize(
        self,
        url: str,
        *args: Any,
        stack_data: Optional[StackStoreModel] = None,
        **kwargs: Any,
    ) -> "LocalStackStore":
        """Initializes a local stack store instance.

        Args:
            url: URL of local directory of the repository to use for
                stack storage.
            stack_data: optional stack data store object to pre-populate the
                stack store with.
            args: additional positional arguments (ignored).
            kwargs: additional keyword arguments (ignored).

        Returns:
            The initialized stack store instance.
        """
        self._url = url
        self._root = self.get_path_from_url(url)
        fileio.create_dir_recursive_if_not_exists(str(self._root))

        if stack_data is not None:
            self.__store = stack_data
            self._write_store()
        elif fileio.file_exists(self._store_path()):
            config_dict = yaml_utils.read_yaml(self._store_path())
            self.__store = StackStoreModel.parse_obj(config_dict)
        else:
            self.__store = StackStoreModel.empty_store()
            self._write_store()

        super().initialize(url, *args, **kwargs)
        return self

    # Public interface implementations:

    @property
    def type(self) -> StoreType:
        """The type of stack store."""
        return StoreType.LOCAL

    @property
    def url(self) -> str:
        """URL of the repository."""
        return self._url

    @staticmethod
    def get_path_from_url(url: str) -> Optional[Path]:
        """Get the path from a URL.

        Args:
            url: The URL to get the path from.

        Returns:
            The path from the URL.
        """
        if not LocalStackStore.is_valid_url(url):
            raise ValueError(f"Invalid URL for local store: {url}")
        url = url.replace("file://", "")
        return Path(url)

    @staticmethod
    def get_local_url(path: str) -> str:
        """Get a local URL for a given local path."""
        return f"file://{path}"

    @staticmethod
    def is_valid_url(url: str) -> bool:
        """Check if the given url is a valid local path."""
        scheme = re.search("^([a-z0-9]+://)", url)
        return not scheme or scheme.group() == "file://"

    def is_empty(self) -> bool:
        """Check if the stack store is empty."""
        return len(self.__store.stacks) == 0

    def get_stack_configuration(
        self, name: str
    ) -> Dict[StackComponentType, str]:
        """Fetches a stack configuration by name.

        Args:
            name: The name of the stack to fetch.

        Returns:
            Dict[StackComponentType, str] for the requested stack name.

        Raises:
            KeyError: If no stack exists for the given name.
        """
        logger.debug("Fetching stack with name '%s'.", name)
        if name not in self.__store.stacks:
            raise KeyError(
                f"Unable to find stack with name '{name}'. Available names: "
                f"{set(self.__store.stacks)}."
            )

        return self.__store.stacks[name]

    @property
    def stack_configurations(self) -> Dict[str, Dict[StackComponentType, str]]:
        """Configuration for all stacks registered in this stack store.

        Returns:
            Dictionary mapping stack names to Dict[StackComponentType, str]
        """
        return self.__store.stacks.copy()

    def register_stack_component(
        self,
        component: StackComponentWrapper,
    ) -> None:
        """Register a stack component.

        Args:
            component: The component to register.

        Raises:
            StackComponentExistsError: If a stack component with the same type
                and name already exists.
        """
        components = self.__store.stack_components[component.type]
        if component.name in components:
            raise StackComponentExistsError(
                f"Unable to register stack component (type: {component.type}) "
                f"with name '{component.name}': Found existing stack component "
                f"with this name."
            )

        # write the component configuration file
        component_config_path = self._get_stack_component_config_path(
            component_type=component.type, name=component.name
        )
        fileio.create_dir_recursive_if_not_exists(
            os.path.dirname(component_config_path)
        )
        write_file_contents_as_string(
            component_config_path,
            base64.b64decode(component.config).decode(),
        )

        # add the component to the stack store dict and write it to disk
        components[component.name] = component.flavor
        self._write_store()
        logger.info(
            "Registered stack component with type '%s' and name '%s'.",
            component.type,
            component.name,
        )

    def deregister_stack(self, name: str) -> None:
        """Remove a stack from storage.

        Args:
            name: The name of the stack to be deleted.
        """
        try:
            del self.__store.stacks[name]
            self._write_store()
            logger.info("Deregistered stack with name '%s'.", name)
        except KeyError:
            logger.warning(
                "Unable to deregister stack with name '%s': No stack exists "
                "with this name.",
                name,
            )

    # Private interface implementations:

    def _create_stack(
        self, name: str, stack_configuration: Dict[StackComponentType, str]
    ) -> None:
        """Add a stack to storage.

        Args:
            name: The name to save the stack as.
            stack_configuration: Dict[StackComponentType, str] to persist.
        """
        self.__store.stacks[name] = stack_configuration
        self._write_store()
        logger.info("Registered stack with name '%s'.", name)

    def _get_component_flavor_and_config(
        self, component_type: StackComponentType, name: str
    ) -> Tuple[str, bytes]:
        """Fetch the flavor and configuration for a stack component.

        Args:
            component_type: The type of the component to fetch.
            name: The name of the component to fetch.

        Returns:
            Pair of (flavor, congfiguration) for stack component, as string and
            base64-encoded yaml document, respectively

        Raises:
            KeyError: If no stack component exists for the given type and name.
        """
        components: Dict[str, str] = self.__store.stack_components[
            component_type
        ]
        if name not in components:
            raise KeyError(
                f"Unable to find stack component (type: {component_type}) "
                f"with name '{name}'. Available names: {set(components)}."
            )

        component_config_path = self._get_stack_component_config_path(
            component_type=component_type, name=name
        )
        flavor = components[name]
        config = base64.b64encode(
            read_file_contents_as_string(component_config_path).encode()
        )
        return flavor, config

    def _get_stack_component_names(
        self, component_type: StackComponentType
    ) -> List[str]:
        """Get names of all registered stack components of a given type."""
        return list(self.__store.stack_components[component_type])

    def _delete_stack_component(
        self, component_type: StackComponentType, name: str
    ) -> None:
        """Remove a StackComponent from storage.

        Args:
            component_type: The type of component to delete.
            name: Then name of the component to delete.
        """
        components = self.__store.stack_components[component_type]
        try:
            del components[name]
            self._write_store()
            logger.info(
                "Deregistered stack component (type: %s) with name '%s'.",
                component_type.value,
                name,
            )
        except KeyError:
            logger.warning(
                "Unable to deregister stack component (type: %s) with name "
                "'%s': No stack component exists with this name.",
                component_type.value,
                name,
            )
        component_config_path = self._get_stack_component_config_path(
            component_type=component_type, name=name
        )

        if fileio.file_exists(component_config_path):
            fileio.remove(component_config_path)

    # User, project and role management

    @property
    def users(self) -> List[User]:
        """All registered users.

        Returns:
            A list of all registered users.
        """
        return self.__store.users

    def create_user(self, user_name: str) -> User:
        """Creates a new user.

        Args:
            user_name: Unique username.

        Returns:
             The newly created user.
        """
        if _get_unique_entity(
            user_name, collection=self.__store.users, ensure_exists=False
        ):
            raise RuntimeError(f"User with name '{user_name}' already exists.")

        user = User(name=user_name)
        self.__store.users.append(user)
        self._write_store()
        return user

    def delete_user(self, user_name: str) -> None:
        """Deletes a user.

        Args:
            user_name: Name of the user to delete.
        """
        user = _get_unique_entity(
            user_name, collection=self.__store.users, ensure_exists=False
        )
        if user:
            self.__store.users.remove(user)
            for user_names in self.__store.team_assignments.values():
                user_names.discard(user.name)

            self.__store.role_assignments = [
                assignment
                for assignment in self.__store.role_assignments
                if assignment.user_id != user.id
            ]
            self._write_store()
            logger.info("Deleted user %s.", user)
        else:
            logger.debug(
                "User '%s' doesn't exist, not deleting anything.", user_name
            )

    @property
    def teams(self) -> List[Team]:
        """All registered teams.

        Returns:
            A list of all registered teams.
        """
        return self.__store.teams

    def create_team(self, team_name: str) -> Team:
        """Creates a new team.

        Args:
            team_name: Unique team name.

        Returns:
             The newly created team.
        """
        if _get_unique_entity(
            team_name, collection=self.__store.teams, ensure_exists=False
        ):
            raise RuntimeError(f"Team with name '{team_name}' already exists.")

        team = Team(name=team_name)
        self.__store.teams.append(team)
        self._write_store()
        return team

    def delete_team(self, team_name: str) -> None:
        """Deletes a team.

        Args:
            team_name: Name of the team to delete.
        """
        team = _get_unique_entity(
            team_name, collection=self.__store.teams, ensure_exists=False
        )
        if team:
            self.__store.teams.remove(team)
            self.__store.team_assignments.pop(team.name, None)
            self.__store.role_assignments = [
                assignment
                for assignment in self.__store.role_assignments
                if assignment.team_id != team.id
            ]
            self._write_store()
            logger.info("Deleted team %s.", team)
        else:
            logger.debug(
                "Team '%s' doesn't exist, not deleting anything.", team_name
            )

    def add_user_to_team(self, team_name: str, user_name: str) -> None:
        """Adds a user to a team.

        Args:
            team_name: Name of the team.
            user_name: Name of the user.
        """
        team = _get_unique_entity(team_name, self.__store.teams)
        user = _get_unique_entity(user_name, self.__store.users)
        self.__store.team_assignments[team.name].add(user.name)
        self._write_store()

    def remove_user_from_team(self, team_name: str, user_name: str) -> None:
        """Removes a user from a team.

        Args:
            team_name: Name of the team.
            user_name: Name of the user.
        """
        team = _get_unique_entity(team_name, self.__store.teams)
        user = _get_unique_entity(user_name, self.__store.users)
        self.__store.team_assignments[team.name].remove(user.name)
        self._write_store()

    @property
    def projects(self) -> List[Project]:
        """All registered projects.

        Returns:
            A list of all registered projects.
        """
        return self.__store.projects

    def create_project(
        self, project_name: str, description: Optional[str] = None
    ) -> Project:
        """Creates a new project.

        Args:
            project_name: Unique project name.
            description: Optional project description.

        Returns:
             The newly created project.
        """
        if _get_unique_entity(
            project_name, collection=self.__store.projects, ensure_exists=False
        ):
            raise RuntimeError(
                f"Project with name '{project_name}' already exists."
            )

        project = Project(name=project_name, description=description)
        self.__store.projects.append(project)
        self._write_store()
        return project

    def delete_project(self, project_name: str) -> None:
        """Deletes a project.

        Args:
            project_name: Name of the project to delete.
        """
        project = _get_unique_entity(
            project_name, collection=self.__store.projects, ensure_exists=False
        )
        if project:
            self.__store.projects.remove(project)
            self.__store.role_assignments = [
                assignment
                for assignment in self.__store.role_assignments
                if assignment.project_id != project.id
            ]

            self._write_store()
            logger.info("Deleted project %s.", project)
        else:
            logger.debug(
                "Project '%s' doesn't exist, not deleting anything.",
                project_name,
            )

    @property
    def roles(self) -> List[Role]:
        """All registered roles.

        Returns:
            A list of all registered roles.
        """
        return self.__store.roles

    @property
    def role_assignments(self) -> List[RoleAssignment]:
        """All registered role assignments.

        Returns:
            A list of all registered role assignments.
        """
        return self.__store.role_assignments

    def create_role(self, role_name: str) -> Role:
        """Creates a new role.

        Args:
            role_name: Unique role name.

        Returns:
             The newly created role.
        """
        if _get_unique_entity(
            role_name, collection=self.__store.roles, ensure_exists=False
        ):
            raise RuntimeError(f"Role with name '{role_name}' already exists.")

        role = Role(name=role_name)
        self.__store.roles.append(role)
        self._write_store()
        return role

    def delete_role(self, role_name: str) -> None:
        """Deletes a role.

        Args:
            role_name: Name of the role to delete.
        """
        role = _get_unique_entity(
            role_name, collection=self.__store.roles, ensure_exists=False
        )
        if role:
            self.__store.roles.remove(role)
            self.__store.role_assignments = [
                assignment
                for assignment in self.__store.role_assignments
                if assignment.role_id != role.id
            ]

            self._write_store()
            logger.info("Deleted role %s.", role)
        else:
            logger.debug(
                "Role '%s' doesn't exist, not deleting anything.", role_name
            )

    def assign_role(
        self,
        role_name: str,
        entity_name: str,
        project_name: Optional[str] = None,
        is_user: bool = True,
    ) -> None:
        """Assigns a role to a user or team.

        Args:
            role_name: Name of the role to assign.
            entity_name: User or team name.
            project_name: Optional project name.
            is_user: Boolean indicating whether the given `entity_name` refers
                to a user.
        """
        role = _get_unique_entity(role_name, collection=self.__store.roles)
        project = _get_unique_entity(
            project_name, collection=self.__store.projects
        )

        if is_user:
            user = _get_unique_entity(entity_name, self.__store.users)
            assignment = RoleAssignment(
                role_id=role.id, project_id=project.id, user_id=user.id
            )
        else:
            team = _get_unique_entity(entity_name, self.__store.teams)
            assignment = RoleAssignment(
                role_id=role.id, project_id=project.id, team_id=team.id
            )

        self.__store.role_assignments.append(assignment)
        self._write_store()

    def revoke_role(
        self,
        role_name: str,
        entity_name: str,
        project_name: Optional[str] = None,
        is_user: bool = True,
    ) -> None:
        """Revokes a role from a user or team.

        Args:
            role_name: Name of the role to revoke.
            entity_name: User or team name.
            project_name: Optional project name.
            is_user: Boolean indicating whether the given `entity_name` refers
                to a user.
        """
        role = _get_unique_entity(role_name, collection=self.__store.roles)

        user_id: Optional[UUID] = None
        team_id: Optional[UUID] = None
        project_id: Optional[UUID] = None

        if is_user:
            user_id = _get_unique_entity(entity_name, self.__store.users).id
        else:
            team_id = _get_unique_entity(entity_name, self.__store.teams).id

        if project_name:
            project_id = _get_unique_entity(
                project_name, collection=self.__store.projects
            ).id

        assignments = self._filter_role_assignments(
            role_id=role.id,
            user_id=user_id,
            team_id=team_id,
            project_id=project_id,
        )
        if assignments:
            self.__store.role_assignments.remove(
                assignments[0]
            )  # there should only be one
            self._write_store()

    def get_users_for_team(self, team_name: str) -> List[User]:
        """Fetches all users of a team.

        Args:
            team_name: Name of the team.

        Returns:
            List of users that are part of the team.
        """
        team = _get_unique_entity(team_name, collection=self.__store.teams)
        user_names = self.__store.team_assignments[team.name]
        return [user for user in self.users if user.name in user_names]

    def get_teams_for_user(self, user_name: str) -> List[Team]:
        """Fetches all teams for a user.

        Args:
            user_name: Name of the user.

        Returns:
            List of teams that the user is part of.
        """
        user = _get_unique_entity(user_name, collection=self.__store.users)
        team_names = [
            team_name
            for team_name, user_names in self.__store.team_assignments.items()
            if user.name in user_names
        ]
        return [team for team in self.teams if team.name in team_names]

    def get_role_assignments_for_user(
        self,
        user_name: str,
        project_name: Optional[str] = None,
        include_team_roles: bool = True,
    ) -> List[RoleAssignment]:
        """Fetches all role assignments for a user.

        Args:
            user_name: Name of the user.
            project_name: Optional filter to only return roles assigned for
                this project.
            include_team_roles: If `True`, includes roles for all teams that
                the user is part of.

        Returns:
            List of role assignments for this user.
        """
        user = _get_unique_entity(user_name, collection=self.__store.users)
        project_id = (
            _get_unique_entity(
                project_name, collection=self.__store.projects
            ).id
            if project_name
            else None
        )
        assignments = self._filter_role_assignments(
            user_id=user.id, project_id=project_id
        )

        if include_team_roles:
            for team in self.get_teams_for_user(user_name):
                assignments += self.get_role_assignments_for_team(
                    team.name, project_name=project_name
                )
        return assignments

    def get_role_assignments_for_team(
        self,
        team_name: str,
        project_name: Optional[str] = None,
    ) -> List[RoleAssignment]:
        """Fetches all role assignments for a team.

        Args:
            team_name: Name of the user.
            project_name: Optional filter to only return roles assigned for
                this project.

        Returns:
            List of role assignments for this team.
        """
        team = _get_unique_entity(team_name, collection=self.__store.teams)
        project_id = (
            _get_unique_entity(
                project_name, collection=self.__store.projects
            ).id
            if project_name
            else None
        )
        return self._filter_role_assignments(
            team_id=team.id, project_id=project_id
        )

    # Implementation-specific internal methods:

    @property
    def root(self) -> Path:
        """The root directory of the stack store."""
        if not self._root:
            raise RuntimeError(
                "Local stack store has not been initialized. Call `initialize` "
                "before using the store."
            )
        return self._root

    def _get_stack_component_config_path(
        self, component_type: StackComponentType, name: str
    ) -> str:
        """Path to the configuration file of a stack component."""
        path = self.root / component_type.plural / f"{name}.yaml"
        return str(path)

    def _store_path(self) -> str:
        """Path to the repository configuration file."""
        return str(self.root / "stacks.yaml")

    def _write_store(self) -> None:
        """Writes the stack store yaml file."""
        config_dict = json.loads(self.__store.json())
        yaml_utils.write_yaml(self._store_path(), config_dict)

    def _filter_role_assignments(
        self,
        role_id: Optional[UUID] = None,
        project_id: Optional[UUID] = None,
        user_id: Optional[UUID] = None,
        team_id: Optional[UUID] = None,
    ) -> List[RoleAssignment]:
        matches = []
        for assignment in self.__store.role_assignments:
            if (
                (role_id and assignment.role_id != role_id)
                or (project_id and project_id != assignment.project_id)
                or (user_id and user_id != assignment.user_id)
                or (team_id and team_id != assignment.team_id)
            ):
                continue
            matches.append(assignment)

        return matches
