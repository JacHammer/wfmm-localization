"""
Script for importing localizations to database
Usage: python3 convert.py [psql|sqlite3] [creds.json]
"""
import os
import json
import sqlite3
import psycopg2
import sys
import xml.etree.ElementTree as ET


SUPPORTED_LANGUAGES = ['en', 'cn']
SUPPORTED_REGIONS = ['eu', 'ru']


def create_connection(db_type, credentials):
    """ create a database connection to the database
        specified by db_file
    :param db_type: type of db; can either be psql or sqlite3
    :param credentials: credentials in dictionary to connect the specified db:
    {
    "psql_db_name": "your_db_name",
    "psql_user": "your_postgres_user",
    "psql_password": "your_password",
    "psql_host": "127.0.0.1",
    "psql_port": "5432",
    "sqlite3_db_dir": "marketplace.db"
    }
    :return: Connection object or None
    """
    connection = None
    if db_type not in ["psql", "sqlite3"]:
        raise NotImplementedError("{} database not implemented.".format(db_type))

    if db_type == "sqlite3":
        try:
            connection = sqlite3.connect(credentials["sqlite3_db_dir"])
        except sqlite3.Error as e:
            print(e)
    if db_type == "psql":
        dbname = credentials['psql_db_name']
        user = credentials['psql_user']
        password = credentials['psql_password']
        host = credentials['psql_host']
        port = credentials['psql_port']
        try:

            connection = psycopg2.connect(dbname=dbname,
                                          user=user,
                                          password=password,
                                          host=host,
                                          port=port)
        except psycopg2.Error as e:
            print("{error} when creating psql connections".format(error=e))

    return connection


def get_weapon_name_by_shortened_key(parsed_xml, key):
    """
    Get the text of shortened weapon name in localization file.
    :param parsed_xml: An xml.etree.ElementTree.ElementTree object. Created by ET.parse()
    :param key: The shortened weapon name in localization file, e.g. shg52, sr47 etc.
    :return: Name of the weapon if it can be found in the localization file; else return ''.
    """

    xml_root = parsed_xml.getroot()
    lookup_string = './/entry[@key="{item_name}"]'.format(item_name=key + '_shop_name')
    lookup_result = xml_root.find(lookup_string)
    if lookup_result is None:
        return ''
    else:
        return list(lookup_result)[1].attrib['value']


def get_camouflage_translation_mapping(connection, locale='en', region='eu'):
    """
    Get the mapping of entity_id to name of the weapon camos.
    Because the entity_id and item_id mapping in eu is different than in ru,
    we need to specify region so that the db gets the correct mapping.
    :param connection: A valid db connection.
    :param locale: select language for key-name translation. Default is 'en'.
    :param region: select which marketplace region translation are imported into. Default is 'eu'.
    :return: A dictionary consisting mapping of entity_id to weapon+camo name.
    """

    mapping = {}

    # TODO: extract this to individual func
    if locale not in SUPPORTED_LANGUAGES or region not in SUPPORTED_REGIONS:
        raise NotImplementedError("{lang} in {region} region is not supported".format(lang=locale, region=region))

    # create xml object
    xml_file = ET.parse(locale+'/text_weapons.xml')
    xml_root = xml_file.getroot()

    # fetch all entities with camouflages
    cursor = connection.cursor()
    if region == 'eu':
        cursor.execute('SELECT * FROM items WHERE kind = \'camouflage\'')
    elif region == 'ru':
        cursor.execute('SELECT * FROM items_ru WHERE kind = \'camouflage\'')

    items = cursor.fetchall()

    # find real name of weapon camo
    for item in items:
        item_id = item[0]
        item_name = item[1]
        item_weapon_name = item_name.split('_')[0]
        lookup_string = './/entry[@key="{item_name}"]'.format(item_name=item_name + '_name')
        lookup_result = xml_root.find(lookup_string)

        # item name is not found, assign empty string
        if lookup_result is None:
            mapping[item_id] = [item_name, '', '']
        # found item
        else:
            mapping[item_id] = [item_name,
                                get_weapon_name_by_shortened_key(xml_file,
                                                                 item_weapon_name)
                                + ' '
                                + list(lookup_result)[1].attrib['value']]
    return mapping


def get_weapon_translation_mapping(connection, locale='en', region='eu'):
    """
    Get the mapping of entity_id to name of weapons.
    :param connection: A valid db connection.
    :param locale: select language for key-name translation. Default is 'en'.
    :param region: select which marketplace region translation are imported into. Default is 'eu'.
    :return: A dictionary consisting mapping of entity_id to weapon name.
    """

    # initialize mapping
    mapping = {}
    if locale not in SUPPORTED_LANGUAGES or region not in SUPPORTED_REGIONS:
        raise NotImplementedError("{lang} in {region} region is not supported".format(lang=locale, region=region))

    # create xml object
    xml_file = ET.parse(locale+'/text_weapons.xml')
    xml_root = xml_file.getroot()

    # fetch all entities with camouflages
    cursor = connection.cursor()
    if region == 'eu':
        cursor.execute('SELECT * FROM items WHERE kind = \'weapon\'')
    if region == 'ru':
        cursor.execute('SELECT * FROM items_ru WHERE kind = \'weapon\'')
    items = cursor.fetchall()

    # find real name of weapon
    for item in items:
        item_id = item[0]
        item_name = item[1]
        if '_shop' in item_name:
            lookup_string = './/entry[@key="{item_name}"]'.format(item_name=item_name + '_name')
        else:
            lookup_string = './/entry[@key="{item_name}"]'.format(item_name=item_name + '_shop_name')
        lookup_result = xml_root.find(lookup_string)

        if lookup_result is None:
            mapping[item_id] = [item_name, '']
        else:
            mapping[item_id] = [item_name,
                                list(lookup_result)[1].attrib['value']]
    return mapping


def get_body_skin_translation_mapping(connection, locale='en', region='eu'):
    """
    Get the mapping of entity_id to body skins.
    :param connection: A valid db connection.
    :param locale: select language for key-name translation. Default is 'en'.
    :param region: select which marketplace region translation are imported into. Default is 'eu'.
    :return: A dictionary consisting mapping of entity_id and entity name.
    """

    # initialize mapping
    mapping = {}
    if locale not in SUPPORTED_LANGUAGES or region not in SUPPORTED_REGIONS:
        raise NotImplementedError("{lang} in {region} region is not supported".format(lang=locale, region=region))

    # create xml object
    xml_file = ET.parse(locale+'/text_armors.xml')
    xml_root = xml_file.getroot()

    # fetch all body skin entities
    cursor = connection.cursor()
    if region == 'eu':
        cursor.execute('SELECT * FROM items WHERE kind = \'appearance\'')
    elif region == 'ru':
        cursor.execute('SELECT * FROM items_ru WHERE kind = \'appearance\'')
    items = cursor.fetchall()

    # find real name of body skins
    for item in items:
        item_id = item[0]
        item_name = item[1]
        lookup_string = './/entry[@key="{item_name}"]'.format(item_name='ui_armor_' + item_name + '_name')
        lookup_result = xml_root.find(lookup_string)

        if lookup_result is None:
            mapping[item_id] = [item_name, '']
        else:
            mapping[item_id] = [item_name,
                                list(lookup_result)[1].attrib['value']]
    return mapping


def get_gear_translation_mapping(connection, locale='en', region='eu'):
    """
    Get the mapping of entity_id to gears.
    :param connection: A valid db connection
    :param locale: select language for key-name translation. Default is 'en'.
    :param region: select which marketplace region translation are imported into. Default is 'eu'.
    :return: A dictionary consisting mapping of entity_id and entity name.
    """

    # initialize mapping
    mapping = {}
    if locale not in SUPPORTED_LANGUAGES:
        print('Unsupported language for translation.')
        return mapping

    # create xml object
    xml_file = ET.parse(locale+'/text_armors.xml')
    xml_root = xml_file.getroot()

    # fetch all gear entities
    cursor = connection.cursor()
    if region == 'eu':
        cursor.execute('SELECT * FROM items WHERE kind = \'equipment\'')
    elif region == 'ru':
        cursor.execute('SELECT * FROM items_ru WHERE kind = \'equipment\'')
    items = cursor.fetchall()

    # find real name of gears
    for item in items:
        item_id = item[0]
        item_name = item[1]

        lookup_string = './/entry[@key="{item_name}"]'.format(item_name='ui_armor_' + item_name + '_name')
        lookup_result = xml_root.find(lookup_string)

        if lookup_result is None:
            mapping[item_id] = [item_name, '']
        else:
            mapping[item_id] = [item_name,
                                list(lookup_result)[1].attrib['value']]
    return mapping


def update_item_translation(connection, entity_id, translation, locale='en', region='eu'):
    """
    Update new translation: entity_id -> translation to the database.
    :param connection: A valid db connection.
    :param entity_id: entity_id in its {region}.
                      note: same item name but different entity_id may occur in different region.
    :param translation: translation in {locale} for {region}
    :param locale: select language for key-name translation. Default is 'en'.
    :param region: select which marketplace region translation are imported into Default is 'eu.
    :return: A dictionary consisting mapping of entity_id and entity name.
    """

    if locale not in SUPPORTED_LANGUAGES or region not in SUPPORTED_REGIONS:
        raise NotImplementedError("{lang} in {region} region is not supported".format(lang=locale, region=region))

    cursor = connection.cursor()
    if type(cursor) == sqlite3.Cursor:
        if region == 'eu':
            if locale == 'en':
                cursor.execute('''UPDATE items SET title_en=? WHERE entity_id=?;''', (translation, entity_id))
            elif locale == 'cn':
                cursor.execute('''UPDATE items SET title_cn=? WHERE entity_id=?;''', (translation, entity_id))
        elif region == 'ru':
            if locale == 'en':
                cursor.execute('''UPDATE items_ru SET title_en=? WHERE entity_id=?;''', (translation, entity_id))
            elif locale == 'cn':
                cursor.execute('''UPDATE items_ru SET title_cn=? WHERE entity_id=?;''', (translation, entity_id))
    elif type(cursor) == psycopg2.extensions.cursor:
        if region == 'eu':
            if locale == 'en':
                cursor.execute('''UPDATE items SET title_en=%s WHERE entity_id=%s;''', (translation, entity_id))
            elif locale == 'cn':
                cursor.execute('''UPDATE items SET title_cn=%s WHERE entity_id=%s;''', (translation, entity_id))
        elif region == 'ru':
            if locale == 'en':
                cursor.execute('''UPDATE items_ru SET title_en=%s WHERE entity_id=%s;''', (translation, entity_id))
            elif locale == 'cn':
                cursor.execute('''UPDATE items_ru SET title_cn=%s WHERE entity_id=%s;''', (translation, entity_id))


def main(db_type, credential_file):
    # open credential file and read it to a dictionary
    dir_path = os.path.dirname(os.path.realpath(__file__))
    with open('{dir_path}/{cred}'.format(dir_path=dir_path, cred=credential_file), 'r') as json_file:
        credentials_string = json_file.read()
    credentials = json.loads(credentials_string)

    # connect to specified db with credentials
    connection = create_connection(db_type, credentials)
    c = connection.cursor()

    # check DB language support
    sql_check_tables = ['''alter table items add title_original text;''',
                        '''alter table items add title_en text;''',
                        '''alter table items add title_cn text;''',
                        '''alter table items_ru add title_original text;'''
                        '''alter table items_ru add title_en text;''',
                        '''alter table items_ru add title_cn text;'''
                        ]
    if db_type == "sqlite3":
        for sql in sql_check_tables:
            try:
                c.execute(sql)
            except sqlite3.OperationalError:
                print('column title_cn already exists in ru')
        connection.commit()
    elif db_type == "psql":
        for sql in sql_check_tables:
            try:
                c.execute(sql)
            except Exception as e:
                print('column title_cn already exists in ru')
        connection.commit()

    # insert translations
    for region in SUPPORTED_REGIONS:
        for language in SUPPORTED_LANGUAGES:
            m = get_camouflage_translation_mapping(connection, locale=language, region=region)
            n = get_weapon_translation_mapping(connection, locale=language, region=region)
            o = get_body_skin_translation_mapping(connection, locale=language, region=region)
            p = get_gear_translation_mapping(connection, locale=language, region=region)
            mappings = [m, n, o, p]

            for translation_mapping in mappings:
                for k, v in translation_mapping.items():
                    print(k, v)
                    update_item_translation(connection, k, v[1], locale=language, region=region)
        connection.commit()
    connection.close()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("option: convert.py [psql|sqlite3] [creds.json]")
        exit(1)
    input_db = sys.argv[1]
    if input_db not in ["psql", "sqlite3"]:
        print("option: convert.py [psql|sqlite3] [creds.json]")
        exit(1)
    cred_file = sys.argv[2]
    main(input_db, cred_file)
