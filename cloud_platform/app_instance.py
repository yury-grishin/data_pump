from typing import TypedDict

from _globals import *
from _logging import log
from cloud_platform import DBaaSAggregator, DBInfo
from cloud_platform import PostgresBackupDaemon


class DataConfigItem(TypedDict):
    classifier: dict
    is_cloneable: bool


class MappedData(TypedDict):
    database: str
    username: str


class MappingItem(DataConfigItem, total=False):
    source: MappedData
    target: MappedData


class AppInstance:
    """Class AppInstance"""
    _MAPPING_MARK: str = '$$'

    def __init__(self, name: str | None = ..., namespace: str = ..., role: str | None = None, tenants: dict = ..., **kwargs):
        self._name = name if isinstance(name, str) else ''
        # 'namespace' property
        if isinstance(namespace, str) and namespace:
            self._namespace = namespace
        else:
            raise ValueError("The 'namespace' value must be a non-empty string.")
        # 'role' property
        if role in ('source', 'target', None):
            self._role = role
        else:
            raise ValueError("The 'role' value must be in ('source', 'target') string.")
        # 'tenants' property
        if isinstance(tenants, dict) and tenants:
            self._tenants = tenants
        else:
            raise ValueError("The 'tenants' value must be a non-empty dictionary.")
        self._dbaas: DBaaSAggregator | None = None
        self._pg_backup: PostgresBackupDaemon | None = None
        self._dbaas_dblist: list[DBInfo] | None = None
        self._data_config: dict | None = None
        self._is_mapping_uptodate: bool = False
        self._mapping: dict | None = None

    @property
    def name(self):
        return self._name

    @property
    def namespace(self):
        return self._namespace

    @property
    def role(self):
        return self._role

    def set_dbaas(self, service: DBaaSAggregator):
        self._dbaas = service

    def set_pg_backup(self, service: PostgresBackupDaemon):
        self._pg_backup = service

    def load_data_config(self, config: dict):
        # TODO: Validate structure of config and format for {'microservice: {'dbtype': [DataConfigItem, ...], ...}, ...}
        self._data_config = config.copy()
        self._is_mapping_uptodate = False

    def get_data_config(self):
        if self._data_config is None:
            self._create_data_config()
        return self._data_config.copy()

    def load_mapping(self, config: dict):
        # TODO: Validate structure of config  and format for {'microservice: {'dbtype': [MappingItem, ...], ...}, ...}
        self._mapping = config.copy()
        self._is_mapping_uptodate = False

    def get_mapping(self):
        return self._mapping.copy()

    def mapping_update(self):
        if not self._mapping:
            self._create_mapping_by_config()

    @property
    def _databases(self) -> list[DBInfo, ...]:
        """Gets if not defined and returns Info of databases from DBaaS"""
        if self._dbaas_dblist is None:
            self._dbaas_dblist = self._dbaas.database_list(self.namespace)
        return self._dbaas_dblist

    def _create_data_config(self):
        """Defines 'self._data_config' by 'self._databases'"""
        self._data_config = {}
        for db in self._databases:
            # Grows config tree: {'microservice: {'dbtype': [], ...}, ...}
            if db['microservice'] not in self._data_config:
                self._data_config[db['microservice']] = {}
            if db['dbtype'] not in self._data_config[db['microservice']]:
                self._data_config[db['microservice']][db['dbtype']] = []
            # Constructs and appends the db config record
            db_cfg: DataConfigItem = {
                'classifier': self._mask_classifier(db['classifier'].copy()),
                'is_cloneable': False
            }
            self._data_config[db['microservice']][db['dbtype']].append(db_cfg)

    def _create_mapping_by_config(self):
        """Recreate the _mapping by _data_config if loaded"""
        if not self._data_config:
            log.warning(f"'{self.__class__.__name__}._data_config' is empty: {self._data_config}")
            return None
        self._mapping = {}
        for microservice in self._data_config:
            for db_type in self._data_config[microservice]:
                for db_cfg in self._data_config[microservice][db_type]:
                    if db_cfg['is_cloneable']:
                        # Grows config tree: {'microservice: {'dbtype': [], ...}, ...}
                        if microservice not in self._mapping:
                            self._mapping[microservice] = {}
                        if db_type not in self._mapping[microservice]:
                            self._mapping[microservice][db_type] = []
                        # Constructs and appends the db config record
                        db_map: MappingItem = {
                            'classifier': db_cfg['classifier'],
                            'is_cloneable': True,
                            'source': {},
                            'target': {}
                        }
                        self._mapping[microservice][db_type].append(db_map)
        self._is_mapping_uptodate = True


    # TODO: def _validate_instance_by_mapping()

    # def get_mapping_by_classifier(self, dbtype: str, classifier: dict) -> dict:
    #     dbaas_rec = self._dbaas.get_by_classifier(self.namespace, dbtype, classifier)

    # def get_db_list_by_config(self, data_config: dict) -> list:
    #
    #     ...

    def _mask_classifier(self, classifier: dict) -> dict:
        """"Masks the real database classifier as a configuration db classifier"""
        if 'namespace' in classifier:
            classifier['namespace'] = f'{self._MAPPING_MARK}app-namespace'
        if 'tenantId' in classifier:
            for tenant_name in self._tenants:
                if classifier['tenantId'] == self._tenants[tenant_name]:
                    classifier['tenantId'] = f'{self._MAPPING_MARK}{tenant_name}'
        return classifier

    def _resolve_classifier(self, classifier: dict) -> dict:
        """Resolves the configuration (masked) db classifier as a real database classifier"""
        mark_len = len(self._MAPPING_MARK)
        if 'namespace' in classifier:
            classifier['namespace'] = self.namespace
        if 'tenantId' in classifier:
            tenant_name = classifier['tenantId'][mark_len:]
            try:
                classifier['tenantId'] = self._tenants[tenant_name]
            except KeyError as err:
                log.error(f"Tenant name missing in config: {repr(err)}")
        return classifier
