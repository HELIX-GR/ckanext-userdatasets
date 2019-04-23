import logging
import ckan.plugins as plugins
import ckan.lib.plugins as lib_plugins
import ckan.lib.dictization.model_save as model_save

from ckan.logic import check_access, get_action, ValidationError, NotFound
import ckan.logic as logic
import ckan.lib.base as base

from ckan.common import _

from ckan.logic.validators import owner_org_validator as default_oov
from ckanext.userdatasets.logic.validators import owner_org_validator as uds_oov
import ckanext.helix.lib.helpers as ext_helpers

log = logging.getLogger(__name__)
abort = base.abort

def package_create(context, data_dict):
    model = context['model']
    user = context['user']
    package_type = data_dict.get('type')
    #package_type = 'harvest'
    package_plugin = lib_plugins.lookup_package_plugin(package_type)
    if 'schema' in context:
        schema = context['schema']
    else:
        schema = package_plugin.create_package_schema()
    schema = package_plugin.create_package_schema()
    # We modify the schema here to replace owner_org_validator by our own
    if 'owner_org' in schema:
        schema['owner_org'] = [uds_oov if f is default_oov else f for f in schema['owner_org']]
    check_access('package_create', context, data_dict)
    if 'api_version' not in context:
        # check_data_dict() is deprecated. If the package_plugin has a
        # check_data_dict() we'll call it, if it doesn't have the method we'll
        # do nothing.
        check_data_dict = getattr(package_plugin, 'check_data_dict', None)
        if check_data_dict:
            try:
                check_data_dict(data_dict, schema)
            except TypeError:
                # Old plugins do not support passing the schema so we need
                # to ensure they still work
                package_plugin.check_data_dict(data_dict)

    #if data_dict['dataset_type'] + '.subject_closed' not in data_dict and 
    data, errors = lib_plugins.plugin_validate(
        package_plugin, context, data_dict, schema, 'package_create')
    log.debug('package_create validate_errs=%r user=%s package=%s data=%r',
              errors, context.get('user'),
              data.get('name'), data_dict)


    if errors:
        model.Session.rollback()
        raise ValidationError(errors)

    rev = model.repo.new_revision()
    rev.author = user
    if 'message' in context:
        rev.message = context['message']
    else:
        rev.message = _(u'REST API: Create object %s') % data.get("name")

    admins = []
    if user:
        user_obj = model.User.by_name(user.decode('utf8'))
        if user_obj:
            admins = [user_obj]
            data['creator_user_id'] = user_obj.id

    pkg = model_save.package_dict_save(data, context)

    #model.setup_default_user_roles(pkg, admins)
    # Needed to let extensions know the package id
    model.Session.flush()
    data['id'] = pkg.id

    context_org_update = context.copy()
    context_org_update['ignore_auth'] = True
    context_org_update['defer_commit'] = True
    get_action('package_owner_org_update')(context_org_update,
                                            {'id': pkg.id,
                                             'organization_id': pkg.owner_org})

    # add dataset in corresponding topics(groups) based on subject
    log.debug('package type %s', package_type)
    #add_to_topic(context, data_dict)

    for item in plugins.PluginImplementations(plugins.IPackageController):
        item.create(pkg)

        item.after_create(context, data)

    if not context.get('defer_commit'):
        model.repo.commit()

    ## need to let rest api create
    context["package"] = pkg
    ## this is added so that the rest controller can make a new location
    context["id"] = pkg.id
    log.debug('Created object %s' % pkg.name)

    # Make sure that a user provided schema is not used on package_show
    context.pop('schema', None)

    return_id_only = context.get('return_id_only', False)

    output = context['id'] if return_id_only \
        else get_action('package_show')(context, {'id': context['id']})

    return output


def add_to_topic(context,data_dict):
    if data_dict['type']!='harvest' and 'closed_tag' in data_dict:
        groups = ext_helpers.topicsMatch(data_dict['closed_tag'])
        
        context_group_update = context.copy()
        context_group_update['ignore_auth'] = True
        context_group_update['defer_commit'] = True
        for group in groups:
            group_data_dict = {"id": group,
                                "object": data_dict['id'],
                                "object_type": 'package',
                                "capacity": 'public'}
            try:
                logic.get_action('member_create')(context_group_update, group_data_dict)
            except NotFound:
                abort(404, _('Group not found'))    
        
        if 'group_id' in data_dict and data_dict['group_id']:  
            #log.debug('group id %s',data_dict['group_id'])
            # add dataset in chosen community(group)
            community_data_dict = {"id": data_dict['group_id'],
                                    "object": data_dict['id'],
                                    "object_type": 'package',
                                    "capacity": 'public'}
            try:
                logic.get_action('member_create')(context_group_update, community_data_dict)
            except NotFound:
                    abort(404, _('Community not found'))        