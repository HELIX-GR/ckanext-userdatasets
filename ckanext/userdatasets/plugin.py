import importlib
import ckan.plugins as p

config = {}
import logging
log1 = logging.getLogger(__name__)

class UserDatasetsPlugin(p.SingletonPlugin):
    """"UserDatasetsPlugin

    This plugin replaces dataset and resource authentication calls to allow
    users with the 'Member' role to create datasets, and edit/delete their
    own datasets (but not others).
    """

    p.implements(p.IAuthFunctions)
    p.implements(p.IActions)
    p.implements(p.IConfigurable)

    def configure(self, main_config):
        """Implementation of IConfigurable.configure"""
        config['default_auth_module'] = config.get('userdatasets.default_auth_module', 'ckan.logic.auth')
        config['default_action_module'] = config.get('userdatasets.default_action_module', 'ckan.logic.action')

    def get_auth_functions(self):
        """Implementation of IAuthFunctions.get_auth_functions"""
        # We override all of create/update/delete for packages, resources and resource views.
        auth_functions = {}
        for action in ['create', 'update', 'delete']:
            default_module = importlib.import_module(config['default_auth_module'] + '.' + action)
            uds_module = importlib.import_module('ckanext.userdatasets.logic.auth.' + action)
            for atype in ['package', 'resource', 'resource_view']:
                fn_name = atype + '_' + action
                if hasattr(default_module, fn_name) and hasattr(uds_module, fn_name):
                    auth_functions[fn_name] = getattr(uds_module, fn_name)
        for fn_name in ['organization_update', 'bulk_update_public', 'bulk_update_private', 'bulk_update_delete']:
            default_module = importlib.import_module(config['default_auth_module'] + '.' + 'update')
            uds_module = importlib.import_module('ckanext.userdatasets.logic.auth.' + 'update')
            if hasattr(default_module, fn_name) and hasattr(uds_module, fn_name):
                    auth_functions[fn_name] = getattr(uds_module, fn_name)
        #auth_functions['organization_update'] = 'ckanext.userdatasets.logic.auth.update.organization_update' 
        
<<<<<<< HEAD
=======
        #log1.info('\nIN GET AUTH , auth_functions is %s\n', auth_functions)
>>>>>>> upstream/master
        return auth_functions

    def get_actions(self):
        """Implementation of IActions.get_actions"""
        actions = {}
        # Override selected actions.
        to_override = [
            ('create', ['package_create']),
            ('update', ['package_update']),
            ('update', ['bulk_update_public']),
            ('get', ['organization_list_for_user'])
        ]
        for override in to_override:
            uds_module = importlib.import_module('ckanext.userdatasets.logic.action.' + override[0])
            for fn_name in override[1]:
                actions[fn_name] = getattr(uds_module, fn_name)

        return actions


def get_default_auth(ftype, function_name):
    """Return the default auth function

    @param type: The type of auth function (create/update/delete)
    @param function: Name of function. It must exists.
    @return: The auth function
    """
    default_module = importlib.import_module(config['default_auth_module'] + '.' + ftype)
    return getattr(default_module, function_name)


def get_default_action(ftype, function_name):
    """Return the default action function

    @param type: The type of action function (create/update/get)
    @param function: Name of function. It must exists.
    @return: The action function
    """
    default_module = importlib.import_module(config['default_action_module'] + '.' + ftype)
    return getattr(default_module, function_name)
