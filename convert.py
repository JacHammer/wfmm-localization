import sqlite3
import xml.etree.ElementTree as ET


SUPPORTED_LANGUAGES = ['en', 'cn']


def create_connection(db_file):
    """ create a database connection to the SQLite database
        specified by db_file
    :param db_file: database file
    :return: Connection object or None
    """
    connection = None
    try:
        connection = sqlite3.connect(db_file)
    except sqlite3.Error as e:
        print(e)

    return connection


def insert_item_to_table(connection, item):
    sql = ('INSERT INTO items(entity_id, item_id, kind, entity_type)\n'
           '             VALUES(?, ?, ?, ?)')
    cur = connection.cursor()
    cur.execute(sql, item)
    return cur.lastrowid


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


def get_camouflage_translation_mapping(connection, locale='en'):
    """
    Get the mapping of entity_id to name of the weapon camos.
    :param connection: A valid SQLite3 DB connection.
    :param locale: select language for key-name translation. Default is 'en'.
    :return: A dictionary consisting mapping of entity_id and entity name.
    """

    mapping = {}
    if locale not in SUPPORTED_LANGUAGES:
        print('Unsupported language for translation.')
        return mapping

    # create xml object
    xml_file = ET.parse(locale+'/text_weapons.xml')
    xml_root = xml_file.getroot()

    # fetch all entities with camouflages
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM items WHERE kind = \'camouflage\'')
    items = cursor.fetchall()

    # find real name of weapon camo
    for item in items:
        item_id = item[0]
        item_name = item[1]
        item_weapon_name = item_name.split('_')[0]
        lookup_string = './/entry[@key="{item_name}"]'.format(item_name=item_name + '_name')
        lookup_result = xml_root.find(lookup_string)

        if lookup_result is None:
            mapping[item_id] = [item_name, '', '']
        else:
            mapping[item_id] = [item_name,
                                get_weapon_name_by_shortened_key(xml_file,
                                                                 item_weapon_name)
                                + ' '
                                + list(lookup_result)[1].attrib['value']]
    return mapping


def get_weapon_translation_mapping(connection, locale='en'):
    """
    Get the mapping of entity_id to name of weapons.
    :param connection: A valid SQLite3 DB connection.
    :param locale: select language for key-name translation. Default is 'en'.
    :return: A dictionary consisting mapping of entity_id and entity name.
    """

    # initialize mapping
    mapping = {}
    if locale not in SUPPORTED_LANGUAGES:
        print('Unsupported language for translation.')
        return mapping

    # create xml object
    xml_file = ET.parse(locale+'/text_weapons.xml')
    xml_root = xml_file.getroot()

    # fetch all entities with camouflages
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM items WHERE kind = \'weapon\'')
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


def get_body_skin_translation_mapping(connection, locale='en'):
    """
    Get the mapping of entity_id to body skins.
    :param connection: A valid SQLite3 DB connection.
    :param locale: select language for key-name translation. Default is 'en'.
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

    # fetch all body skin entities
    cursor = connection.cursor()
    cursor.execute('SELECT * FROM items WHERE kind = \'appearance\'')
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


def get_gear_translation_mapping(connection, locale='en'):
    """
    Get the mapping of entity_id to gears.
    :param connection: A valid SQLite3 DB connection
    :param locale: select language for key-name translation. Default is 'en'.
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
    cursor.execute('SELECT * FROM items WHERE kind = \'equipment\'')
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


def update_item_translation(connection, entity_id, translation, locale='en'):

    if locale not in SUPPORTED_LANGUAGES:
        print("ERROR: Unsupported language in database!")
        raise NotImplementedError

    cursor = connection.cursor()
    if locale == 'en':
        cursor.execute('''UPDATE items SET title_en=? WHERE entity_id=?;''', (translation, entity_id))
    elif locale == 'cn':
        cursor.execute('''UPDATE items SET title_cn=? WHERE entity_id=?;''', (translation, entity_id))


conn = create_connection('../wfmb/marketplace.db')
c = conn.cursor()

# check DB language support
try:
    c.execute('''alter table items add title_original text;''')
except sqlite3.OperationalError:
    print('column title_original already exists')
try:
    c.execute('''alter table items add title_en text;''')
except sqlite3.OperationalError:
    print('column title_en already exists')
try:
    c.execute('''alter table items add title_cn text;''')
except sqlite3.OperationalError:
    print('column title_cn already exists')
conn.commit()

# check DB insert functionality
test_item = (6942069420, 'legit_weapon_name', 'weapon', 'inventory')
try:
    insert_item_to_table(conn, test_item)
except sqlite3.IntegrityError:
    print('test_item already exists')
conn.commit()

# insert translations
for language in SUPPORTED_LANGUAGES:
    m = get_camouflage_translation_mapping(conn, locale=language)
    n = get_weapon_translation_mapping(conn, locale=language)
    o = get_body_skin_translation_mapping(conn, locale=language)
    p = get_gear_translation_mapping(conn, locale=language)
    mappings = [m, n, o, p]

    for translation_mapping in mappings:
        for k, v in translation_mapping.items():
            print(k, v)
            update_item_translation(conn, k, v[1], locale=language)
conn.commit()
conn.close()
