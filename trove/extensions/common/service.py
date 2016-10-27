# Copyright [2015] Hewlett-Packard Development Company, L.P.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.


import abc

from oslo_config.cfg import NoSuchOptError
from oslo_log import log as logging
from oslo_utils import importutils
from oslo_utils import strutils
import six

from trove.cluster import models as cluster_models
from trove.cluster.models import DBCluster
from trove.cluster import views as cluster_views
import trove.common.apischema as apischema
from trove.common import cfg
from trove.common import exception
from trove.common.i18n import _LI
from trove.common import notification
from trove.common.notification import StartNotification
from trove.common import policy
from trove.common import remote
from trove.common import utils
from trove.common.utils import correct_id_with_req
from trove.common import wsgi
from trove.datastore import models as datastore_models
from trove.extensions.common import models as common_utils
from trove.extensions.common import views
from trove.instance import models as instance_models
from trove.instance.models import DBInstance


LOG = logging.getLogger(__name__)
import_class = importutils.import_class
CONF = cfg.CONF


class ExtensionController(wsgi.Controller):

    @classmethod
    def authorize_target_action(cls, context, target_rule_name,
                                target_id, is_cluster=False):
        target = None
        if is_cluster:
            target = cluster_models.Cluster.load(context, target_id)
        else:
            target = instance_models.Instance.load(context, target_id)

        if not target:
            if is_cluster:
                raise exception.ClusterNotFound(cluster=target_id)
            raise exception.InstanceNotFound(instance=target_id)

        target_type = 'cluster' if is_cluster else 'instance'
        policy.authorize_on_target(
            context, '%s:extension:%s' % (target_type, target_rule_name),
            {'tenant': target.tenant_id})


@six.add_metaclass(abc.ABCMeta)
class BaseDatastoreRootController(ExtensionController):
    """Base class that defines the contract for root controllers."""

    @abc.abstractmethod
    def root_index(self, req, tenant_id, instance_id, is_cluster):
        pass

    @abc.abstractmethod
    def root_create(self, req, body, tenant_id, instance_id, is_cluster):
        pass

    @abc.abstractmethod
    def root_delete(self, req, tenant_id, instance_id, is_cluster):
        pass

    @staticmethod
    def _get_password_from_body(body=None):
        if body:
            return body['password'] if 'password' in body else None
        return None


class DefaultRootController(BaseDatastoreRootController):

    def root_index(self, req, tenant_id, instance_id, is_cluster):
        """Returns True if root is enabled; False otherwise."""
        if is_cluster:
            raise exception.ClusterOperationNotSupported(
                operation='show_root')
        LOG.info(_LI("Getting root enabled for instance '%s'.") % instance_id)
        LOG.info(_LI("req : '%s'\n\n") % req)
        context = req.environ[wsgi.CONTEXT_KEY]
        is_root_enabled = common_utils.Root.load(context, instance_id)
        return wsgi.Result(views.RootEnabledView(is_root_enabled).data(), 200)

    def root_create(self, req, body, tenant_id, instance_id, is_cluster):
        if is_cluster:
            raise exception.ClusterOperationNotSupported(
                operation='enable_root')
        LOG.info(_LI("Enabling root for instance '%s'.") % instance_id)
        LOG.info(_LI("req : '%s'\n\n") % req)
        context = req.environ[wsgi.CONTEXT_KEY]
        user_name = context.user
        password = DefaultRootController._get_password_from_body(body)
        root = common_utils.Root.create(context, instance_id,
                                        user_name, password)
        return wsgi.Result(views.RootCreatedView(root).data(), 200)

    def root_delete(self, req, tenant_id, instance_id, is_cluster):
        if is_cluster:
            raise exception.ClusterOperationNotSupported(
                operation='disable_root')
        LOG.info(_LI("Disabling root for instance '%s'.") % instance_id)
        LOG.info(_LI("req : '%s'\n\n") % req)
        context = req.environ[wsgi.CONTEXT_KEY]
        is_root_enabled = common_utils.Root.load(context, instance_id)
        if not is_root_enabled:
            raise exception.UserNotFound(uuid="root")
        common_utils.Root.delete(context, instance_id)
        return wsgi.Result(None, 200)


class ClusterRootController(DefaultRootController):

    def root_index(self, req, tenant_id, instance_id, is_cluster):
        """Returns True if root is enabled; False otherwise."""
        if is_cluster:
            return self.cluster_root_index(req, tenant_id, instance_id)
        else:
            return self.instance_root_index(req, tenant_id, instance_id)

    def instance_root_index(self, req, tenant_id, instance_id):
        LOG.info(_LI("Getting root enabled for instance '%s'.") % instance_id)
        LOG.info(_LI("req : '%s'\n\n") % req)
        context = req.environ[wsgi.CONTEXT_KEY]
        try:
            is_root_enabled = common_utils.ClusterRoot.load(
                context,
                instance_id)
        except exception.UnprocessableEntity:
            raise exception.UnprocessableEntity(
                "Cluster %s is not ready." % instance_id)
        return wsgi.Result(views.RootEnabledView(is_root_enabled).data(), 200)

    def cluster_root_index(self, req, tenant_id, cluster_id):
        LOG.info(_LI("Getting root enabled for cluster '%s'.") % cluster_id)
        single_instance_id, cluster_instances = self._get_cluster_instance_id(
            tenant_id, cluster_id)
        return self.instance_root_index(req, tenant_id, single_instance_id)

    def root_create(self, req, body, tenant_id, instance_id, is_cluster):
        if is_cluster:
            return self.cluster_root_create(req, body, tenant_id, instance_id)
        else:
            return self.instance_root_create(req, body, instance_id)

    def instance_root_create(self, req, body, instance_id,
                             cluster_instances=None):
        LOG.info(_LI("Enabling root for instance '%s'.") % instance_id)
        LOG.info(_LI("req : '%s'\n\n") % req)
        context = req.environ[wsgi.CONTEXT_KEY]
        user_name = context.user
        password = ClusterRootController._get_password_from_body(body)
        root = common_utils.ClusterRoot.create(context, instance_id, user_name,
                                               password, cluster_instances)
        return wsgi.Result(views.RootCreatedView(root).data(), 200)

    def cluster_root_create(self, req, body, tenant_id, cluster_id):
        LOG.info(_LI("Enabling root for cluster '%s'.") % cluster_id)
        single_instance_id, cluster_instances = self._get_cluster_instance_id(
            tenant_id, cluster_id)
        return self.instance_root_create(req, body, single_instance_id,
                                         cluster_instances)

    def _find_cluster_node_ids(self, tenant_id, cluster_id):
        args = {'tenant_id': tenant_id, 'cluster_id': cluster_id}
        cluster_instances = DBInstance.find_all(**args).all()
        return [db_instance.id for db_instance in cluster_instances]

    def _get_cluster_instance_id(self, tenant_id, cluster_id):
        instance_ids = self._find_cluster_node_ids(tenant_id, cluster_id)
        single_instance_id = instance_ids[0]
        return single_instance_id, instance_ids


class RootController(ExtensionController):
    """Controller for instance functionality."""

    def index(self, req, tenant_id, instance_id):
        """Returns True if root is enabled; False otherwise."""
        datastore_manager, is_cluster = self._get_datastore(tenant_id,
                                                            instance_id)
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'root:index', instance_id,
                                     is_cluster=is_cluster)
        root_controller = self.load_root_controller(datastore_manager)
        return root_controller.root_index(req, tenant_id, instance_id,
                                          is_cluster)

    def create(self, req, tenant_id, instance_id, body=None):
        """Enable the root user for the db instance."""
        datastore_manager, is_cluster = self._get_datastore(tenant_id,
                                                            instance_id)
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'root:create', instance_id,
                                     is_cluster=is_cluster)
        root_controller = self.load_root_controller(datastore_manager)
        if root_controller is not None:
            return root_controller.root_create(req, body, tenant_id,
                                               instance_id, is_cluster)
        else:
            raise NoSuchOptError('root_controller', group='datastore_manager')

    def delete(self, req, tenant_id, instance_id):
        datastore_manager, is_cluster = self._get_datastore(tenant_id,
                                                            instance_id)
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'root:delete', instance_id,
                                     is_cluster=is_cluster)
        root_controller = self.load_root_controller(datastore_manager)
        if root_controller is not None:
            return root_controller.root_delete(req, tenant_id,
                                               instance_id, is_cluster)
        else:
            raise NoSuchOptError

    def _get_datastore(self, tenant_id, instance_or_cluster_id):
        """
        Returns datastore manager and a boolean
        showing if instance_or_cluster_id is a cluster id
        """
        args = {'id': instance_or_cluster_id, 'tenant_id': tenant_id}
        is_cluster = False
        try:
            db_info = DBInstance.find_by(**args)
        except exception.ModelNotFoundError:
            is_cluster = True
            db_info = DBCluster.find_by(**args)

        ds_version = (datastore_models.DatastoreVersion.
                      load_by_uuid(db_info.datastore_version_id))
        ds_manager = ds_version.manager
        return (ds_manager, is_cluster)

    def load_root_controller(self, manager):
        try:
            clazz = CONF.get(manager).get('root_controller')
            LOG.debug("Loading Root Controller class %s." % clazz)
            root_controller = import_class(clazz)
            return root_controller()
        except NoSuchOptError:
            return None


class RoutingController(ExtensionController):

    NAME = None

    def get_controller(self, tenant_id, target_id):
        datastore_manager, is_cluster = self.get_manager(
            tenant_id, target_id)
        return (self.load_controller(datastore_manager), is_cluster)

    def get_manager(self, tenant_id, target_id):
        args = {'id': target_id, 'tenant_id': tenant_id}
        is_cluster = False
        try:
            db_info = DBInstance.find_by(**args)
        except exception.ModelNotFoundError:
            is_cluster = True
            db_info = DBCluster.find_by(**args)

        ds_version = (datastore_models.DatastoreVersion.
                      load_by_uuid(db_info.datastore_version_id))
        ds_manager = ds_version.manager
        return (ds_manager, is_cluster)

    @classmethod
    def load_controller(cls, manager):
        clazz = cfg.get_configuration_property(cls.NAME, manager)
        LOG.debug("Loading controller class: %s" % clazz)
        controller = import_class(clazz)
        return controller()


class RoutingUserController(RoutingController):

    schemas = apischema.user
    NAME = 'user_controller'

    @classmethod
    def get_schema(cls, action, body):
        action_schema = super(RoutingUserController, cls).get_schema(
            action, body)
        if 'update_all' == action:
            update_type = list(body.keys())[0]
            action_schema = action_schema.get(update_type, {})
        return action_schema

    def index(self, req, tenant_id, instance_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_index(req, tenant_id, instance_id)
        return controller.index(req, tenant_id, instance_id)

    def create(self, req, body, tenant_id, instance_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_create(req, body, tenant_id, instance_id)
        return controller.create(req, body, tenant_id, instance_id)

    def delete(self, req, tenant_id, instance_id, id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_delete(req, tenant_id, instance_id, id)
        return controller.delete(req, tenant_id, instance_id, id)

    def show(self, req, tenant_id, instance_id, id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_show(req, tenant_id, instance_id, id)
        return controller.show(req, tenant_id, instance_id, id)

    def update(self, req, body, tenant_id, instance_id, id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_update(
                req, body, tenant_id, instance_id, id)
        return controller.update(req, body, tenant_id, instance_id, id)

    def update_all(self, req, body, tenant_id, instance_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_update_all(
                req, body, tenant_id, instance_id)
        return controller.update_all(req, body, tenant_id, instance_id)


class DatastoreController(ExtensionController):

    def create_guest_client(self, context, instance_id):
        common_utils.load_and_verify(context, instance_id)
        return remote.create_guest_client(context, instance_id)

    def get_coordinator_node_id(self, req, tenant_id, cluster_id):
        context = req.environ[wsgi.CONTEXT_KEY]
        cluster = cluster_models.Cluster.load(context, cluster_id)
        cluster_view = cluster_views.load_view(cluster, req)
        cluster_instances, _ = cluster_view.build_instances()
        instance_ids = [instance['id'] for instance in cluster_instances]

        if not instance_ids:
            exception.TroveError(
                _("This cluster does not have any API access nodes."))

        return instance_ids[0]


@six.add_metaclass(abc.ABCMeta)
class DatastoreUserController(DatastoreController):

    @abc.abstractmethod
    def build_model_view(self, user_model):
        """Build view from a given user model."""
        return None

    @abc.abstractmethod
    def build_model_collection_view(self, user_models):
        """Build view from a given collection of user models."""
        return None

    def index(self, req, tenant_id, instance_id):
        LOG.info(_LI("Listing users for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:index', instance_id)

        users, next_marker = self.list_users(context, instance_id)
        filtered_users = filter(
            lambda user: not self.is_reserved_id(self.get_user_id(user)),
            users)
        view = self.build_model_collection_view(
            filtered_users).paginated(req.url, next_marker)

        return wsgi.Result(view.data(), 200)

    def is_reserved_id(self, user_id):
        """Return whether a given identifier is reserved.
        Reserved identifiers cannot be operated on and will be excluded from
        listings.
        """
        return False

    def list_users(self, context, instance_id):
        client = self.create_guest_client(context, instance_id)
        limit = utils.pagination_limit(context.limit, CONF.users_page_size)
        data, next_marker = client.list_users(
            limit=limit, marker=context.marker, include_marker=False)
        return self.parse_users_from_response(data), next_marker

    def parse_users_from_response(self, user_data):
        return [self.parse_user_from_response(item) for item in user_data]

    @abc.abstractmethod
    def parse_user_from_response(self, user_data):
        """Create user model from guest response data."""
        return None

    def create(self, req, body, tenant_id, instance_id):
        LOG.info(_LI("Creating users for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n"
                     "body: '%(body)s'\n'n") %
                 {"id": instance_id,
                  "req": strutils.mask_password(req),
                  "body": strutils.mask_password(body)})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:create', instance_id)

        context.notification = notification.DBaaSUserCreate(context,
                                                            request=req)
        users = body['users']
        usernames = [user['name'] for user in users]
        client = self.create_guest_client(context, instance_id)
        with StartNotification(context, instance_id=instance_id,
                               username=",".join(usernames)):

            try:
                user_models = self.parse_users_from_request(users)
                unique_user_ids = set()
                for model in user_models:
                    user_id = self.get_user_id(model)
                    if self.is_reserved_id(user_id):
                        raise exception.ReservedUserId(name=user_id)
                    if user_id in unique_user_ids:
                        raise exception.DuplicateUserId(name=user_id)
                    if self.find_user(client, user_id):
                        raise exception.UserAlreadyExists(name=user_id)
                    unique_user_ids.add(user_id)

                self.create_users(client, user_models)
            except (ValueError, AttributeError) as e:
                raise exception.BadRequest(str(e))

        return wsgi.Result(None, 202)

    def parse_users_from_request(self, user_data):
        return [self.parse_user_from_request(item) for item in user_data]

    @abc.abstractmethod
    def parse_user_from_request(self, user_data):
        """Create user model from API request data."""
        return None

    def get_user_id(self, user_model):
        """Return a string used to uniquely identify the user on the instance.
        """
        return user_model.name

    def find_user(self, client, user_id):
        username, hostname = self.parse_user_id(user_id)
        data = client.get_user(username=username, hostname=hostname)
        if data:
            return self.parse_user_from_response(data)
        return None

    def parse_user_id(self, user_id):
        """Parse a given user id string to name and hostname (if any)."""
        return user_id, None

    def create_users(self, client, user_models):
        return client.create_user(
            users=[model.serialize() for model in user_models])

    def delete(self, req, tenant_id, instance_id, id):
        LOG.info(_LI("Delete instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:delete', instance_id)

        user_id = correct_id_with_req(id, req)
        context.notification = notification.DBaaSUserDelete(context,
                                                            request=req)

        client = self.create_guest_client(context, instance_id)
        with StartNotification(context, instance_id=instance_id,
                               username=user_id):

            try:
                if self.is_reserved_id(user_id):
                    raise exception.ReservedUserId(name=user_id)

                model = self.find_user(client, user_id)
                if not model:
                    raise exception.UserNotFound(uuid=user_id)

                self.delete_user(client, model)
            except (ValueError, AttributeError) as e:
                raise exception.BadRequest(str(e))

        return wsgi.Result(None, 202)

    def delete_user(self, client, user_model):
        return client.delete_user(user=user_model.serialize())

    def show(self, req, tenant_id, instance_id, id):
        LOG.info(_LI("Showing a user for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:show', instance_id)

        user_id = correct_id_with_req(id, req)
        client = self.create_guest_client(context, instance_id)
        try:
            if self.is_reserved_id(user_id):
                raise exception.ReservedUserId(name=user_id)

            model = self.find_user(client, user_id)
            if not model:
                raise exception.UserNotFound(uuid=user_id)

            view = self.build_model_view(model)

            return wsgi.Result(view.data(), 200)
        except (ValueError, AttributeError) as e:
            raise exception.BadRequest(str(e))

    def update(self, req, body, tenant_id, instance_id, id):
        LOG.info(_LI("Updating user attributes for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": strutils.mask_password(req)})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:update', instance_id)

        user_id = correct_id_with_req(id, req)

        updates = body['user']
        context.notification = notification.DBaaSUserUpdateAttributes(
            context, request=req)
        client = self.create_guest_client(context, instance_id)
        with StartNotification(context, instance_id=instance_id,
                               username=user_id):

            try:
                if self.is_reserved_id(user_id):
                    raise exception.ReservedUserId(name=user_id)

                model = self.find_user(client, user_id)
                if not model:
                    raise exception.UserNotFound(uuid=user_id)

                new_user_id = self.apply_user_updates(model, updates)
                if (new_user_id is not None and
                        self.find_user(client, new_user_id)):
                    raise exception.UserAlreadyExists(name=new_user_id)

                self.update_user(client, user_id, updates)
            except (ValueError, AttributeError) as e:
                raise exception.BadRequest(str(e))

        return wsgi.Result(None, 202)

    def apply_user_updates(self, user_model, updates):
        """Apply a set of attributes updates to a given user model.
        Return the new user id string if it was updated or None otherwise.
        """
        id_changed = False
        updated_name = updates.get('name')
        if updated_name is not None:
            user_model.name = updated_name
            id_changed = True
        updated_password = updates.get('password')
        if updated_password is not None:
            user_model.password = updated_password

        return self.get_user_id(user_model) if id_changed else None

    def update_user(self, client, user_id, updates):
        username, hostname = self.parse_user_id(user_id)
        return client.update_attributes(
            username=username, hostname=hostname, user_attrs=updates)

    def update_all(self, req, body, tenant_id, instance_id):
        """Change the password of one or more users."""
        LOG.info(_LI("Updating user password for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": strutils.mask_password(req)})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:update_all', instance_id)

        context.notification = notification.DBaaSUserChangePassword(
            context, request=req)
        users = body['users']
        usernames = [user['name'] for user in users]
        client = self.create_guest_client(context, instance_id)
        with StartNotification(context, instance_id=instance_id,
                               username=",".join(usernames)):

            try:
                user_models = self.parse_users_from_request(users)
                for model in user_models:
                    user_id = self.get_user_id(model)
                    if self.is_reserved_id(user_id):
                        raise exception.ReservedUserId(name=user_id)
                    if not self.find_user(client, user_id):
                        raise exception.UserNotFound(uuid=user_id)

                self.change_passwords(client, user_models)
            except (ValueError, AttributeError) as e:
                raise exception.BadRequest(str(e))

        return wsgi.Result(None, 202)

    def change_passwords(self, client, user_models):
        return client.change_passwords(
            users=[model.serialize() for model in user_models])

    def cluster_index(self, req, tenant_id, cluster_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.index(req, tenant_id, instance_id)

    def cluster_create(self, req, body, tenant_id, cluster_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.create(req, body, tenant_id, instance_id)

    def cluster_delete(self, req, tenant_id, cluster_id, id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.delete(req, tenant_id, instance_id, id)

    def cluster_show(self, req, tenant_id, cluster_id, id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.show(req, tenant_id, instance_id, id)

    def cluster_update(self, req, body, tenant_id, cluster_id, id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.update(req, body, tenant_id, instance_id, id)

    def cluster_update_all(self, req, body, tenant_id, cluster_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.update_all(req, body, tenant_id, instance_id)


class RoutingDatabaseController(RoutingController):

    schemas = apischema.dbschema
    NAME = 'database_controller'

    def index(self, req, tenant_id, instance_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_index(req, tenant_id, instance_id)
        return controller.index(req, tenant_id, instance_id)

    def create(self, req, body, tenant_id, instance_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_create(req, body, tenant_id, instance_id)
        return controller.create(req, body, tenant_id, instance_id)

    def delete(self, req, tenant_id, instance_id, id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_delete(req, tenant_id, instance_id, id)
        return controller.delete(req, tenant_id, instance_id, id)

    def show(self, req, tenant_id, instance_id, id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_show(req, tenant_id, instance_id, id)
        return controller.show(req, tenant_id, instance_id, id)


@six.add_metaclass(abc.ABCMeta)
class DatastoreDatabaseController(DatastoreController):

    @abc.abstractmethod
    def build_model_view(self, database_model):
        """Build view from a given database model."""
        return None

    @abc.abstractmethod
    def build_model_collection_view(self, database_models):
        """Build view from a given collection of database models."""
        return None

    def index(self, req, tenant_id, instance_id):
        LOG.info(_LI("Listing databases for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'database:index', instance_id)

        databases, next_marker = self.list_databases(context, instance_id)
        filtered_databases = filter(
            lambda database: not self.is_reserved_id(
                self.get_database_id(database)), databases)
        view = self.build_model_collection_view(
            filtered_databases).paginated(req.url, next_marker)

        return wsgi.Result(view.data(), 200)

    def is_reserved_id(self, database_id):
        """Return whether a given identifier is reserved.
        Reserved identifiers cannot be operated on and will be excluded from
        listings.
        """
        return False

    def list_databases(self, context, instance_id):
        client = self.create_guest_client(context, instance_id)
        limit = utils.pagination_limit(context.limit, CONF.databases_page_size)
        data, next_marker = client.list_databases(
            limit=limit, marker=context.marker, include_marker=False)
        return self.parse_databases_from_response(data), next_marker

    def parse_databases_from_response(self, database_data):
        return [self.parse_database_from_response(item)
                for item in database_data]

    @abc.abstractmethod
    def parse_database_from_response(self, database_data):
        """Create database model from guest response data."""
        return None

    def create(self, req, body, tenant_id, instance_id):
        LOG.info(_LI("Creating databases for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n"
                     "body: '%(body)s'\n'n") %
                 {"id": instance_id,
                  "req": strutils.mask_password(req),
                  "body": strutils.mask_password(body)})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'database:create', instance_id)

        context.notification = notification.DBaaSDatabaseCreate(context,
                                                                request=req)
        databases = body['databases']
        dbnames = [database['name'] for database in databases]
        client = self.create_guest_client(context, instance_id)
        with StartNotification(context, instance_id=instance_id,
                               dbname=",".join(dbnames)):

            try:
                database_models = self.parse_databases_from_request(databases)
                unique_database_ids = set()
                for model in database_models:
                    database_id = self.get_database_id(model)
                    if self.is_reserved_id(database_id):
                        raise exception.ReservedDatabaseId(name=database_id)
                    if database_id in unique_database_ids:
                        raise exception.DuplicateDatabaseId(name=database_id)
                    if self.find_database(client, database_id):
                        raise exception.DatabaseAlreadyExists(name=database_id)
                    unique_database_ids.add(database_id)

                self.create_databases(client, database_models)
            except (ValueError, AttributeError) as e:
                raise exception.BadRequest(str(e))

        return wsgi.Result(None, 202)

    def parse_databases_from_request(self, database_data):
        return [
            self.parse_database_from_request(item) for item in database_data]

    @abc.abstractmethod
    def parse_database_from_request(self, database_data):
        """Create database model from API request data."""
        return None

    def get_database_id(self, database_model):
        """Return a string used to uniquely identify the database on the
        instance.
        """
        return database_model.name

    def find_database(self, client, database_id):
        # Since that as of Ocata there is not database-show
        # we need to search the whole list.
        dbname = self.parse_database_id(database_id)
        data, _ = client.list_databases(
            limit=None, marker=None, include_marker=True)
        database_models = self.parse_databases_from_response(data)
        for database in database_models:
            if database.name == dbname:
                return database

        return None

    def parse_database_id(self, database_id):
        """Parse a given database id string to components."""
        return database_id

    def create_databases(self, client, database_models):
        return client.create_database(
            databases=[model.serialize() for model in database_models])

    def delete(self, req, tenant_id, instance_id, id):
        LOG.info(_LI("Delete instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'user:delete', instance_id)

        database_id = correct_id_with_req(id, req)
        context.notification = notification.DBaaSDatabaseDelete(context,
                                                                request=req)

        client = self.create_guest_client(context, instance_id)
        with StartNotification(context, instance_id=instance_id,
                               dbname=database_id):

            try:
                if self.is_reserved_id(database_id):
                    raise exception.ReservedDatabaseId(name=database_id)

                model = self.find_database(client, database_id)
                if not model:
                    raise exception.DatabaseNotFound(uuid=database_id)

                self.delete_database(client, model)
            except (ValueError, AttributeError) as e:
                raise exception.BadRequest(str(e))

        return wsgi.Result(None, 202)

    def delete_database(self, client, database_model):
        return client.delete_database(database=database_model.serialize())

    def show(self, req, tenant_id, instance_id, id):
        LOG.info(_LI("Showing a database for instance '%(id)s'\n"
                     "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(context, 'database:show', instance_id)

        database_id = correct_id_with_req(id, req)
        client = self.create_guest_client(context, instance_id)
        try:
            if self.is_reserved_id(database_id):
                raise exception.ReservedDatabaseId(name=database_id)

            model = self.find_database(client, database_id)
            if not model:
                raise exception.DatabaseNotFound(uuid=database_id)

            view = self.build_model_view(model)

            return wsgi.Result(view.data(), 200)
        except (ValueError, AttributeError) as e:
            raise exception.BadRequest(str(e))

    def cluster_index(self, req, tenant_id, cluster_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.index(req, tenant_id, instance_id)

    def cluster_create(self, req, body, tenant_id, cluster_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.create(req, body, tenant_id, instance_id)

    def cluster_delete(self, req, tenant_id, cluster_id, id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.delete(req, tenant_id, instance_id, id)

    def cluster_show(self, req, tenant_id, cluster_id, id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.show(req, tenant_id, instance_id, id)


class RoutingUserAccessController(RoutingController):

    schemas = apischema.dbschema
    NAME = 'user_access_controller'

    def index(self, req, tenant_id, instance_id, user_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_index(
                req, tenant_id, instance_id, user_id)
        return controller.index(req, tenant_id, instance_id, user_id)

    def update(self, req, body, tenant_id, instance_id, user_id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_update(
                req, body, tenant_id, instance_id, user_id)
        return controller.update(req, body, tenant_id, instance_id, user_id)

    def delete(self, req, tenant_id, instance_id, user_id, id):
        controller, is_cluster = self.get_controller(tenant_id, instance_id)
        if is_cluster:
            return controller.cluster_delete(
                req, tenant_id, instance_id, user_id, id)
        return controller.delete(req, tenant_id, instance_id, user_id, id)


@six.add_metaclass(abc.ABCMeta)
class DatastoreUserAccessController(DatastoreController):

    @abc.abstractproperty
    def user_controller(self):
        """Return an instance of DatastoreUserController."""
        return None

    @abc.abstractproperty
    def database_controller(self):
        """Return an instance of DatastoreDatabaseController."""
        return None

    def build_model_collection_view(self, database_models):
        """Build view from a given collection of database models."""
        return self.database_controller.build_model_collection_view(
            database_models)

    def assert_user_show(self, req, tenant_id, instance_id, user_id):
        """Assert that a show requests succeeds on a given user."""
        res = self.user_controller.show(
            req, tenant_id, instance_id, user_id)
        if res and res.status != 200:
            raise exception.TroveError(
                "Unexpected status code from user-show: %s" % res.status)

    def assert_database_show(self, req, tenant_id, instance_id, database_id):
        """Assert that a show requests succeeds on a given database."""
        res = self.database_controller.show(
            req, tenant_id, instance_id, database_id)
        if res and res.status != 200:
            raise exception.TroveError(
                "Unexpected status code from database-show: %s" % res.status)

    def index(self, req, tenant_id, instance_id, user_id):
        LOG.info(_("Showing user access for instance '%(id)s'\n"
                   "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(
            context, 'user_access:index', instance_id)

        user_id = correct_id_with_req(user_id, req)
        self.assert_user_show(req, tenant_id, instance_id, user_id)
        client = self.create_guest_client(context, instance_id)
        databases = self.list_access(client, user_id)
        view = self.build_model_collection_view(databases)

        return wsgi.Result(view.data(), 200)

    def list_access(self, client, user_id):
        username, hostname = self.parse_user_id(user_id)
        return self.parse_databases_from_response(
            client.list_access(username, hostname))

    def parse_databases_from_response(self, database_data):
        """Create database models from guest response data."""
        return self.database_controller.parse_databases_from_response(
            database_data)

    def parse_user_id(self, user_id):
        """Parse a given user id string to name and hostname (if any)."""
        return self.user_controller.parse_user_id(user_id)

    def update(self, req, body, tenant_id, instance_id, user_id):
        LOG.info(_("Granting user access for instance '%(id)s'\n"
                   "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(
            context, 'user_access:update', instance_id)

        user_id = correct_id_with_req(user_id, req)
        context.notification = notification.DBaaSUserGrant(
            context, request=req)
        database_ids = self.parse_database_ids_from_request(body['databases'])
        with StartNotification(context, instance_id=instance_id,
                               username=user_id,
                               database=",".join(database_ids)):
            self.assert_user_show(req, tenant_id, instance_id, user_id)
            for database_id in database_ids:
                self.assert_database_show(
                    req, tenant_id, instance_id, database_id)

            client = self.create_guest_client(context, instance_id)
            self.grant_access(client, user_id, database_ids)

        return wsgi.Result(None, 202)

    def parse_database_ids_from_request(self, database_data):
        return [self.parse_database_id(database['name'])
                for database in database_data]

    def parse_database_id(self, database_id):
        """Parse a given database id string to components."""
        return self.database_controller.parse_database_id(database_id)

    def grant_access(self, client, user_id, database_ids):
        username, hostname = self.parse_user_id(user_id)
        return client.grant_access(username, hostname, database_ids)

    def delete(self, req, tenant_id, instance_id, user_id, id):
        LOG.info(_("Revoking user access for instance '%(id)s'\n"
                   "req : '%(req)s'\n\n") %
                 {"id": instance_id, "req": req})
        context = req.environ[wsgi.CONTEXT_KEY]
        self.authorize_target_action(
            context, 'user_access:delete', instance_id)

        context.notification = notification.DBaaSUserRevoke(
            context, request=req)
        user_id = correct_id_with_req(user_id, req)

        with StartNotification(context, instance_id=instance_id,
                               username=user_id, database=id):
            self.assert_user_show(req, tenant_id, instance_id, user_id)
            client = self.create_guest_client(context, instance_id)
            if not self.has_access(client, user_id, id):
                raise exception.DatabaseNotInAccessList(
                    database_name=id, user_name=user_id)
            self.revoke_access(client, user_id, id)

        return wsgi.Result(None, 202)

    def has_access(self, client, user_id, database_id):
        database_id = self.parse_database_id(database_id)
        user_databases = self.list_access(client, user_id)
        return any(
            self.get_database_id(model) == database_id
            for model in user_databases)

    def get_database_id(self, database_model):
        """Return a string used to uniquely identify the database on the
        instance.
        """
        return self.database_controller.get_database_id(database_model)

    def revoke_access(self, client, user_id, database_id):
        username, hostname = self.parse_user_id(user_id)
        database_id = self.parse_database_id(database_id)
        return client.revoke_access(username, hostname, database_id)

    def cluster_index(self, req, tenant_id, cluster_id, user_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.index(req, tenant_id, instance_id, user_id)

    def cluster_update(self, req, body, tenant_id, cluster_id, user_id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.update(req, body, tenant_id, instance_id, user_id)

    def cluster_delete(self, req, tenant_id, cluster_id, user_id, id):
        instance_id = self.get_coordinator_node_id(req, tenant_id, cluster_id)
        return self.delete(req, tenant_id, instance_id, user_id, id)
