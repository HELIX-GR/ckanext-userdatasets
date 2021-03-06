from ckan.logic.auth import get_package_object, get_resource_object
from ckan.authz import users_role_for_group_or_org, has_user_permission_for_some_org
from ckanext.userdatasets.plugin import get_default_auth
from ckanext.userdatasets.logic.auth.auth import user_owns_package_as_member, user_is_member_of_package_org
import ckan.logic as logic
import logging
log1 = logging.getLogger(__name__)

get_action = logic.get_action
def package_create(context, data_dict):
    user = context['auth_user_obj']
    if data_dict and 'owner_org' in data_dict:
        role = users_role_for_group_or_org(data_dict['owner_org'], user.name)
        if role == 'member':
            return {'success': True}
    else:
        # If there is no organization, then this should return success if the user can create datasets for *some*
        # organisation (see the ckan implementation), so either if anonymous packages are allowed or if we have
        # member status in any organization.
        if has_user_permission_for_some_org(user.name, 'read'):
            return {'success': True}

    fallback = get_default_auth('create', 'package_create')
    return fallback(context, data_dict)


def resource_create(context, data_dict):
    user = context['auth_user_obj']
    model = context['model']

    
    package_id = data_dict.get('package_id')
    if not package_id:  #workaround for editing datasets with no resources
        package_id = data_dict.get('id')
        data_dict['package_id'] = package_id
    package = get_action('package_show')(context, {'id': package_id})
    if user_owns_package_as_member(user, package):
        return {'success': True}
    elif user_is_member_of_package_org(user, package):
        log1.debug('in member org')
        return {'success': False}

    fallback = get_default_auth('create', 'resource_create')
    return fallback(context, data_dict)


def resource_view_create(context, data_dict):
    user = context['auth_user_obj']
    model = context['model']
    # data_dict provides 'resource_id', while get_resource_object expects 'id'. This is not consistent with the rest of
    # the API - so future proof it by catering for both cases in case the API is made consistent (one way or the other)
    # later.
    if data_dict and 'resource_id' in data_dict:
        dc = {'id': data_dict['resource_id'], 'resource_id': data_dict['resource_id']}
    elif data_dict and 'id' in data_dict:
        dc = {'id': data_dict['id'], 'resource_id': data_dict['id']}
    else:
        dc = data_dict
    resource = get_resource_object(context, dc)
    package = model.Package.get(resource.package_id)
    if user_owns_package_as_member(user, package):
        return {'success': True}
    elif user_is_member_of_package_org(user, package):
        return {'success': False}

    fallback = get_default_auth('create', 'resource_view_create')
    return fallback(context, data_dict)
