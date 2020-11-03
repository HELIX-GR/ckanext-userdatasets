import logging
import datetime
import ckan.plugins as plugins
import ckan.lib.plugins as lib_plugins
import ckan.lib.dictization.model_save as model_save

from ckan.logic import check_access, get_action, ValidationError, NotFound
import ckan.lib.base as base
import ckan.logic as logic
import ckan.model as m
from ckan.common import _, c,  config
import ckan.lib.search as search
import json
import uuid

from ckan.logic.validators import owner_org_validator as default_oov
from ckanext.userdatasets.logic.validators import owner_org_validator as uds_oov
import ckanext.helix.lib.helpers as ext_helpers

log = logging.getLogger(__name__)
abort = base.abort


def package_update(context, data_dict):
    model = context['model']
    user = context['user']
    name_or_id = data_dict.get("id") or data_dict['name']

    pkg = model.Package.get(name_or_id)
    if pkg is None:
        raise NotFound(_('Package was not found.'))
    context["package"] = pkg
    data_dict["id"] = pkg.id
    data_dict['type'] = pkg.type

    check_access('package_update', context, data_dict)

    # get the schema
    package_plugin = lib_plugins.lookup_package_plugin(pkg.type)
    if 'schema' in context and context['schema'] is not None:
        schema = context['schema']
    else:
        schema = package_plugin.update_package_schema()

    log.debug('package type %s', data_dict['type'])

    # add dataset in corresponding topics(groups) based on subject
    # add_to_topic()

    # We modify the schema here to replace owner_org_validator by our own
    if 'owner_org' in schema:
        schema['owner_org'] = [
            uds_oov if f is default_oov else f for f in schema['owner_org']]

    if 'api_version' not in context:
        # check_data_dict() is deprecated. If the package_plugin has a
        # check_data_dict() we'll call it, if it doesn't have the method we'll
        # do nothing.
        check_data_dict = getattr(package_plugin, 'check_data_dict', None)
        if check_data_dict:
            try:
                package_plugin.check_data_dict(data_dict, schema)
            except TypeError:
                # Old plugins do not support passing the schema so we need
                # to ensure they still work.
                package_plugin.check_data_dict(data_dict)

    data, errors = lib_plugins.plugin_validate(
        package_plugin, context, data_dict, schema, 'package_update')
    # log.debug('package_update validate_errs=%r user=%s package=%s data=%r',
    #          errors, context.get('user'),
    #          context.get('package').name if context.get('package') else '',
    #          data)

    if errors:
        model.Session.rollback()
        raise ValidationError(errors)

    rev = model.repo.new_revision()
    rev.author = user
    if 'message' in context:
        rev.message = context['message']
    else:
        rev.message = _(u'REST API: Update object %s') % data.get("name")

    # avoid revisioning by updating directly
    model.Session.query(model.Package).filter_by(id=pkg.id).update(
        {"metadata_modified": datetime.datetime.utcnow()})
    model.Session.refresh(pkg)

    pkg = model_save.package_dict_save(data, context)

    context_org_update = context.copy()
    context_org_update['ignore_auth'] = True
    context_org_update['defer_commit'] = True
    get_action('package_owner_org_update')(context_org_update,
                                           {'id': pkg.id,
                                            'organization_id': pkg.owner_org})

    for item in plugins.PluginImplementations(plugins.IPackageController):
        item.edit(pkg)

        item.after_update(context, data)

    if not context.get('defer_commit'):
        model.repo.commit()

    #log.debug('Updated object %s' % pkg.name)

    return_id_only = context.get('return_id_only', False)

    # Make sure that a user provided schema is not used on package_show
    context.pop('schema', None)
    #log.debug('id = %s', data_dict['id'])
    # we could update the dataset so we should still be able to read it.
    context['ignore_auth'] = True
    output = data_dict['id'] if return_id_only \
        else get_action('package_show')(context, {'id': data_dict['id']})

    return output

# Add new uuid for public doi (datasets are now published)


def add_public_doi(datasets):
    context_copy = {'model': m, 'session': m.Session,
                    'user': c.user or c.author, 'auth_user_obj': c.userobj, 'ignore_auth': True}
    for id in datasets:
        dataset = get_action('package_show')(context_copy, {'id': id})
        if 'datacite.public_doi' not in dataset:
            doi = ext_helpers.getDataciteDoi(dataset)
            dataset['datacite.public_doi'] = doi
            package_update(context_copy, dataset)
    return


def _bulk_update_dataset(context, data_dict, update_dict):
    ''' Bulk update shared code for organizations'''

    datasets = data_dict.get('datasets', [])
    org_id = data_dict.get('org_id')

    log.info(' datasets are %s, type %s', datasets, type(datasets))

    model = context['model']
    model.Session.query(model.package_table) \
        .filter(model.Package.id.in_(datasets)) \
        .filter(model.Package.owner_org == org_id) \
        .update(update_dict, synchronize_session=False)

    # revisions
    model.Session.query(model.package_revision_table) \
        .filter(model.PackageRevision.id.in_(datasets)) \
        .filter(model.PackageRevision.owner_org == org_id) \
        .filter(model.PackageRevision.current is True) \
        .update(update_dict, synchronize_session=False)

    model.Session.commit()

    # solr update here
    psi = search.PackageSearchIndex()

    # update the solr index in batches
    BATCH_SIZE = 50

    def process_solr(q):
        # update the solr index for the query
        query = search.PackageSearchQuery()
        q = {
            'q': q,
            'fl': 'data_dict',
            'wt': 'json',
            'fq': 'site_id:"%s"' % config.get('ckan.site_id'),
            'rows': BATCH_SIZE
        }

        for result in query.run(q)['results']:
            data_dict = json.loads(result['data_dict'])
            if data_dict['owner_org'] == org_id:
                data_dict.update(update_dict)
                psi.index_package(data_dict, defer_commit=True)

    count = 0
    q = []
    for id in datasets:
        q.append('id:"%s"' % (id))
        count += 1
        if count % BATCH_SIZE == 0:
            process_solr(' OR '.join(q))
            q = []
    if len(q):
        process_solr(' OR '.join(q))
    # finally commit the changes
    psi.commit()

    # add after bulk updated for session conflicts
    if update_dict['private'] == False:
        add_public_doi(datasets)
        # notify users for their accepted datasets
        ext_helpers.notify_users(datasets)
    


def bulk_update_private(context, data_dict):
    ''' Make a list of datasets private

    :param datasets: list of ids of the datasets to update
    :type datasets: list of strings

    :param org_id: id of the owning organization
    :type org_id: int
    '''

    logic.check_access('bulk_update_private', context, data_dict)
    _bulk_update_dataset(context, data_dict, {'private': True})


def bulk_update_public(context, data_dict):
    ''' Make a list of datasets public

    :param datasets: list of ids of the datasets to update
    :type datasets: list of strings

    :param org_id: id of the owning organization
    :type org_id: int
    '''

    logic.check_access('bulk_update_public', context, data_dict)
    _bulk_update_dataset(context, data_dict, {'private': False})


def add_to_topic(context, data_dict):
    if data_dict['type'] != 'harvest' and 'closed_tag' in data_dict:
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
                logic.get_action('member_create')(
                    context_group_update, group_data_dict)
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
                logic.get_action('member_create')(
                    context_group_update, community_data_dict)
            except NotFound:
                abort(404, _('Community not found'))

