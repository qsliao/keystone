# Copyright 2015 Reliance Jio Infocom
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from keystone.common import sql
from keystone import credential
from keystone import exception

from cqlengine import columns
from cqlengine import connection
from cqlengine import BatchQuery
from cqlengine.management import sync_table
from cqlengine.models import Model
from cqlengine.query import BatchType

class CredentialModel(Model):
    id = columns.Text(primary_key=True, max_length=64)
    user_id = columns.Text(max_length=64)
    project_id = columns.Text(max_length=64)
    blob = columns.Bytes()
    type = columns.Text(max_length=255)
    extra = columns.Bytes()

connection.setup(['127.0.0.1'], 'keystone')

# create your keyspace.  This is, in general, not what you want in production
# see https://cassandra.apache.org/doc/cql3/CQL.html#createKeyspaceStmt for
# options
#from cqlengine.management import create_keyspace
#create_keyspace('keystone', "SimpleStrategy", 1)

sync_table(CredentialModel)



#class CredentialModel(sql.ModelBase, sql.DictBase):
#    __tablename__ = 'credential'
#    attributes = ['id', 'user_id', 'project_id', 'blob', 'type']
#    id = sql.Column(sql.String(64), primary_key=True)
#    user_id = sql.Column(sql.String(64),
#                         nullable=False)
#    project_id = sql.Column(sql.String(64))
#    blob = sql.Column(sql.JsonBlob(), nullable=False)
#    type = sql.Column(sql.String(255), nullable=False)
#    extra = sql.Column(sql.JsonBlob())

def to_dict(model_obj):
    model_dict = {}
    for col_tuple in model_obj:
        model_dict[col_tuple[0]] = col_tuple[1]
    return model_dict

class Credential(credential.Driver):

    # credential crud

    @sql.handle_conflicts(conflict_type='credential')
    def create_credential(self, credential_id, credential):
        #session = sql.get_session()
        #with session.begin():
        #    ref = CredentialModel.from_dict(credential)
        #    session.add(ref)
        #return ref.to_dict()
        columns = CredentialModel._columns.keys()
        #print columns, credential, credential_id
        create_dict = {column: credential.get(column, 'blah') for column in columns}
        #print create_dict
        ref = CredentialModel.create(**create_dict)
        return to_dict(ref)

    @sql.truncated
    def list_credentials(self, hints):
        #session = sql.get_session()
        #credentials = session.query(CredentialModel)
        #credentials = sql.filter_limit_query(CredentialModel,
        #                                     credentials, hints)
        #return [s.to_dict() for s in credentials]
        refs = CredentialModel.objects()
        return [to_dict(ref) for ref in refs]

    def list_credentials_for_user(self, user_id):
        #session = sql.get_session()
        #query = session.query(CredentialModel)
        #refs = query.filter_by(user_id=user_id).all()
        #return [ref.to_dict() for ref in refs]
        refs = CredentialModel.objects(user_id=user_id)
        return [to_dict(ref) for ref in refs]

    def _get_credential(self, credential_id):
        #ref = session.query(CredentialModel).get(credential_id)
        #if ref is None:
        #    raise exception.CredentialNotFound(credential_id=credential_id)
        #return ref
        refs = CredentialModel.objects(id=credential_id)
        if len(refs) is None:
            raise exception.CredentialNotFound(credential_id=credential_id)
        return refs[0]

    def get_credential(self, credential_id):
        #session = sql.get_session()
        #return self._get_credential(session, credential_id).to_dict()
        return to_dict(self._get_credential(credential_id))

    @sql.handle_conflicts(conflict_type='credential')
    def update_credential(self, credential_id, credential):
        #session = sql.get_session()
        #with session.begin():
        #    ref = self._get_credential(session, credential_id)
        #    old_dict = ref.to_dict()
        #    for k in credential:
        #        old_dict[k] = credential[k]
        #    new_credential = CredentialModel.from_dict(old_dict)
        #    for attr in CredentialModel.attributes:
        #        if attr != 'id':
        #            setattr(ref, attr, getattr(new_credential, attr))
        #    ref.extra = new_credential.extra
        #return ref.to_dict()
        ref = self._get_credential(credential_id)
        ref_dict = to_dict(ref)
        for key in credential:
            ref_dict[key] = credential[key]
        ref_id = ref_dict.pop(id)
        ref.objects(id=ref_id).update(ref_dict)
        return ref

    def delete_credential(self, credential_id):
        #session = sql.get_session()

        #with session.begin():
        #    ref = self._get_credential(session, credential_id)
        #    session.delete(ref)
        ref = self._get_credential(credential_id).delete()

    def delete_credentials_for_project(self, project_id):
        #session = sql.get_session()

        #with session.begin():
        #    query = session.query(CredentialModel)
        #    query = query.filter_by(project_id=project_id)
        #    query.delete()
        refs = CredentialModel.objects(project_id=project_id)
        with BatchQuery(batch_type=BatchType.Unlogged) as b:
            for ref in refs:
                CredentialModel.objects(id=ref.id).batch(b).delete()

    def delete_credentials_for_user(self, user_id):
        #session = sql.get_session()

        #with session.begin():
        #    query = session.query(CredentialModel)
        #    query = query.filter_by(user_id=user_id)
        #    query.delete()
        refs = CredentialModel.objects(user_id=user_id)
        with BatchQuery(batch_type=BatchType.Unlogged) as b:
            for ref in refs:
                CredentialModel.objects(id=ref.id).batch(b).delete()
