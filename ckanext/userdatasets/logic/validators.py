import ckan.lib.navl.dictization_functions as df
from ckan.authz import users_role_for_group_or_org
from ckan.logic.validators import owner_org_validator as default_oov
import logging
log1 = logging.getLogger(__name__)
missing = df.missing

def owner_org_validator(key, data, errors, context):
    owner_org = data.get(key)
    user = context['auth_user_obj']
    if owner_org is not missing and owner_org is not None and owner_org != '' and user is not None:
        role = users_role_for_group_or_org(owner_org, user.name)
        if role == 'member':
            return

    default_oov(key, data, errors, context)
