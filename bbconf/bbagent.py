from bbconf.db_utils import POSTGRES_DIALECT, BaseEngine


class BedBaseAgent(object):
    def __init__(
        self,
        host="localhost",
        port=5432,
        database="pep-db",
        user=None,
        password=None,
        drivername=POSTGRES_DIALECT,
        dsn=None,
        echo=False,
    ):
        """
        Initialize connection to the pep_db database. You can use The basic connection parameters
        or libpq connection string.
        :param host: database server address e.g., localhost or an IP address.
        :param port: the port number that defaults to 5432 if it is not provided.
        :param database: the name of the database that you want to connect.
        :param user: the username used to authenticate.
        :param password: password used to authenticate.
        :param drivername: driver of the database [Default: postgresql]
        :param dsn: libpq connection string using the dsn parameter
        (e.g. "localhost://username:password@pdp_db:5432")
        """

        pep_db_engine = BaseEngine(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            drivername=drivername,
            dsn=dsn,
            echo=echo,
        )
        sa_engine = pep_db_engine.engine

        # self.__sa_engine = sa_engine
        #
        # self.__project = PEPDatabaseProject(pep_db_engine)
        # self.__annotation = PEPDatabaseAnnotation(pep_db_engine)
        # self.__namespace = PEPDatabaseNamespace(pep_db_engine)
        # self.__sample = PEPDatabaseSample(pep_db_engine)
        # self.__user = PEPDatabaseUser(pep_db_engine)
        # self.__view = PEPDatabaseView(pep_db_engine)

        return sa_engine
