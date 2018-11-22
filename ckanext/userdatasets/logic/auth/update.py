from ckan.logic.auth import get_package_object, get_resource_object
import ckan.logic.auth as logic_auth
import ckan.authz as authz
from ckan.common import _, c
from ckanext.userdatasets.plugin import get_default_auth
from ckanext.userdatasets.logic.auth.auth import user_owns_package_as_member, user_is_member_of_package_org
from ckanext.userdatasets.logic.auth.auth import get_resource_view_object


def package_update(context, data_dict):
    user = context['auth_user_obj']
    package = get_package_object(context, data_dict)
    if user_owns_package_as_member(user, package):
        return {'success': True}

    fallback = get_default_auth('update', 'package_update')
    return fallback(context, data_dict)


def resource_update(context, data_dict):
    user = context['auth_user_obj']
    model = context['model']
    resource = get_resource_object(context, data_dict)
    package = model.Package.get(resource.package_id)
    if user_owns_package_as_member(user, package):
        return {'success': True}
    elif user_is_member_of_package_org(user, package):
        return {'success': False}

    fallback = get_default_auth('update', 'resource_update')
    return fallback(context, data_dict)


def resource_view_update(context, data_dict):
    user = context['auth_user_obj']
    model = context['model']
    resource_view = get_resource_view_object(context, data_dict)
    resource = get_resource_object(context, {'id': resource_view.resource_id})
    package = model.Package.get(resource.package_id)
    if user_owns_package_as_member(user, package):
        return {'success': True}
    elif user_is_member_of_package_org(user, package):
        return {'success': False}

    fallback = get_default_auth('update', 'resource_view_update')
    return fallback(context, data_dict)

def organization_update(context, data_dict):
<<<<<<< HEAD
    
    group = logic_auth.get_group_object(context, data_dict)
    user = context['user']
=======
    import logging
    log1 = logging.getLogger(__name__)
    log1.info('\nIN CUSTOM ORG UPDATE\n')
    group = logic_auth.get_group_object(context, data_dict)
    user = context['user']
    log1.info('\nIN CUSTOM ORG UPDATE user is %s, context is %s, data dict is %s\n',user, context, data_dict)
>>>>>>> upstream/master
    authorized = authz.has_user_permission_for_group_or_org(
        group.id, user, 'update')
    if data_dict:
        c.user_role = authz.users_role_for_group_or_org(data_dict['id'], user) 
        if c.user_role == 'editor':
            authorized = True
    if not authorized:
        return {'success': False,
                'msg': _('User %s not authorized to edit organization %s') %
                        (user, group.id)}
    else:
        return {'success': True}

def bulk_update_private(context, data_dict):
    org_id = data_dict.get('org_id')
    user = context['user']
    authorized = authz.has_user_permission_for_group_or_org(
        org_id, user, 'update')
    c.user_role = authz.users_role_for_group_or_org(org_id, user) 
    if c.user_role == 'editor':
        authorized = True
    if not authorized:
        return {'success': False}
    return {'success': True}


def bulk_update_public(context, data_dict):
    org_id = data_dict.get('org_id')
    user = context['user']
    authorized = authz.has_user_permission_for_group_or_org(
        org_id, user, 'update')
    c.user_role = authz.users_role_for_group_or_org(org_id, user) 
    if c.user_role == 'editor':
        authorized = True
    if not authorized:
        return {'success': False}
    return {'success': True}


def bulk_update_delete(context, data_dict):
    org_id = data_dict.get('org_id')
    user = context['user']
    authorized = authz.has_user_permission_for_group_or_org(
        org_id, user, 'update')
    c.user_role = authz.users_role_for_group_or_org(org_id, user) 
    if c.user_role == 'editor':
        authorized = True
    if not authorized:
        return {'success': False}
    return {'success': True}
