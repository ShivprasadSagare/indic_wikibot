import os
import tqdm
from utils import *
import bz2
import json
import multiprocessing


#proper key values documentation is presented here: https://www.mediawiki.org/wiki/Wikibase/DataModel/JSON
def get_node_data(node_data):
    data = {}
    #mandatory check for english labels
    if not (is_key_exists('labels', node_data) and is_key_exists('en', node_data['labels'])):
        return False, data
    data.update({
        'en_label': clean_str(node_data['labels']['en']['value']),
        'en_desc': '', 
    })
    
    #optional check for description
    if (is_key_exists('descriptions', node_data) and is_key_exists('en', node_data['descriptions'])):
        data.update({
            'en_desc': clean_str(node_data['descriptions']['en']['value']),
        })
    
    return True, data

def get_all_attributes(node_data):
    node_properties = set()
    status, data = get_node_data(node_data)
    if not status:
        return status, data, node_properties
    #updates the language dependent wikipedia title
    if is_key_exists('sitelinks', node_data) and is_key_exists('enwiki', node_data['sitelinks']):
        data.update({
            'en_wikipedia_title': node_data['sitelinks']['enwiki'].get('title', ''), 
        })
    #update the alias
    if is_key_exists('aliases', node_data) and is_key_exists('en', node_data['aliases']):
        aliases = node_data['aliases']['en']
        node_alias = []
        for a in aliases:
            node_alias.append(a['value'])
        data.update({
            'aliases': node_alias, 
        })
    node_attributes = []
    claims = node_data.get('claims', None)
    if claims is not None:
        for property_id, property_data in claims.items():
            property_id = clean_str(property_id)
            if len(property_data) == 0:
                continue
            for snak in property_data:
                # rank = snak.get('rank', None)
                # if (rank is not None) and not is_key_exists(rank, wiki_config['snak_rank']):
                #     continue
                if not is_key_exists('mainsnak', snak):
                    continue
                mainsnak = snak.get('mainsnak')
                snak_type = mainsnak.get('snaktype', None)
                if snak_type is None or clean_str(snak_type) != 'value':
                    continue
                mainsnak_datatype = mainsnak.get('datatype', None)
                try:
                    if mainsnak_datatype is None or clean_str(mainsnak_datatype) not in ['string', 'monolingualtext']:
                        continue
                    datavalue_type = clean_str(mainsnak['datavalue']['type'])
                    datavalue_entry = mainsnak['datavalue']['value']
                    if datavalue_type == 'string':
                        node_attributes.append([property_id, datavalue_entry])
                        node_properties.add(property_id)
                    if datavalue_type == 'monolingualtext' and datavalue_entry['language']=='en':
                        node_attributes.append([property_id, datavalue_entry['text']])
                        node_properties.add(property_id)

                except Exception as e:
                    logger.error(" unable to process main-snak %s. original exception: %s" % (str(mainsnak), str(e)))
    data.update({
        'attributes': node_attributes,
    })
    return status, data, node_properties

def load_worker_data(logger, worker_config):
    file_path = os.path.join(os.path.abspath(worker_config['store_path']), "%s-{}.txt"%worker_config['name'])
    logger.debug('loading and cleaning up extra files')
    cache_file_names = {'entities': None, 'properties': None}
    for file_name in cache_file_names:
        cache_file = file_path.format(file_name)
        with open(cache_file, 'r') as data_file:
            cache_file_names[file_name] = json.load(data_file)
        logger.debug('successfully loaded - %s - from file : %s' %(file_name, cache_file))
    return cache_file_names['entities'], cache_file_names['properties']

def store_worker_data(worker_config, node_info, new_attributes=[]):
    file_path = os.path.join(os.path.abspath(worker_config['store_path']), "%s-{}.txt"%worker_config['name'])
    logger = worker_config['logger']
    for node_type, node_data in node_info.items():
        cache_file = file_path.format(node_type)
        with open(cache_file, 'w') as data_file:
            json.dump(node_data, data_file)
        logger.debug('successfully stored - %s - to file : %s' %(node_type, cache_file))
    
    if len(new_attributes)!=0:
        with open(file_path.format('attributes'), 'w') as af:
            for attribute in new_attributes:
                af.write("%s\n"%attribute)

def collect_node_data(worker_config):
    start_time = datetime.utcnow()
    
    logger = worker_config.get('logger')
    property_nodes = worker_config.get('properties')
    entity_nodes = worker_config.get('entities')
    
    node_info = {
        'entities': {},
        'properties': {},
    }
    new_attributes = set()
    #offset indicates line from which it start reading wikidata
    def wikidata(filepath, offset=1, step=1):
        with bz2.open(os.path.abspath(filepath), mode='rt') as dump_file:
            line_count=0
            dump_file.read(2) # skip first two bytes: "{\n"
            for line in dump_file:
                line_count+=1
                if offset > line_count or (line_count - offset)%step != 0:
                    continue
                try:
                    yield json.loads(line.rstrip(',\n'))
                except json.decoder.JSONDecodeError:
                    continue
    
    logger.info("started processing node information")
    #clear the previously collected triples , properties and tail_entities 
    logger.info(" print marker is set to : %s" % (int(worker_config['marker'])))
    
    #missing data fields stats
    searchable_entities, searchable_properties = 0, 0
    invalid_entities, inavlid_properties = 0, 0
    marker_start_time = datetime.utcnow()
    counter=0
    for data in wikidata(worker_config['dumpfile'], offset=worker_config['offset'], step=worker_config['step']):
        counter+=1
        if counter % int(worker_config['marker']) == 0:
            marker_delta = (datetime.utcnow() - marker_start_time).total_seconds()
            logger.info(" | %d M | explored new entities - %d and properties - %d. [%d secs]" % ((counter/1e6), searchable_entities, searchable_properties, marker_delta))
            logger.debug(" | %d invalid entities, %d  invalid properties" % (invalid_entities, inavlid_properties))
            marker_start_time = datetime.utcnow()
            if len(node_info['entities'])>=50 and len(node_info['properties'])>=5:
                break
        entity_id = data.get('id', None)
        entity_type = data.get('type', None)
        if entity_id is None or entity_type is None:
            continue
        entity_id, entity_type = clean_str(entity_id), clean_str(entity_type)
        if entity_type=='item' and entity_id in entity_nodes:
            searchable_entities+=1
            if entity_id in worker_config.get('target_nodes'):
                status, info, node_attributes = get_all_attributes(data)
                for attributes in node_attributes:
                    if attributes not in property_nodes:
                        new_attributes.add(attributes)
            else:
                status, info = get_node_data(data)
            if not status:
                invalid_entities+=1
                continue
            node_info['entities'][entity_id] = info
        
        if entity_type=='property' and entity_id in property_nodes:
            searchable_properties+=1
            status, info = get_node_data(data)
            if not status:
                inavlid_properties+=1
                continue
            node_info['properties'][entity_id] = info

    valid_entities = searchable_entities - invalid_entities
    valid_properties =  searchable_properties - inavlid_properties
    logger.info("node information extracted for %d/%d entities and %d/%d properties" % (valid_entities, searchable_entities, valid_properties, searchable_properties))
    time_delta = (datetime.utcnow() - start_time).total_seconds()
    logger.info(" completed in %f secs" % time_delta)
    store_worker_data(worker_config, node_info, new_attributes=list(new_attributes))

def extract_node_data_from_dump(logger, config, properties, entities):
    worker_count = config.get('thread_count', 1)
    global_worker_config = {}
    #configuring workers
    for i in range(1, worker_count+1):
        local_config = {'name': 'worker-%s'%(i),
                        'dumpfile': config.get('wikidata_dump_path'),
                        'offset': i,
                        'step': worker_count,
                        'logger': None,
                        'marker': config.get('marker', 1e6),
                        'store_path': config.get('store_path'),
                        'properties': properties,
                        'entities': entities,
                        'target_nodes': config.get('target_nodes'),
                        }
    
        log_file_path = os.path.join(config.get('log_path', '.'), "%s.log"%local_config['name'])
        local_config.update({'logger': ManualLogger(local_config['name'], log_file_path)})
        global_worker_config[i] = local_config

    start_time = datetime.utcnow()
    logger.info('initiating node information extraction with %d properties %s entities' % (len(properties), len(entities)))
    worker_handler = []
    #assign task to workers
    for worker_id in range(1, worker_count+1):
        prc = multiprocessing.Process(target=collect_node_data, args=(global_worker_config[worker_id],))
        worker_handler.append(prc)
    logger.critical(' spawning %d worker process' % (worker_count))
    #execute the workers
    for worker in worker_handler:
        worker.start()
    #wait for workers to complete
    for worker in worker_handler:
        worker.join()
    time_delta = (datetime.utcnow() - start_time).total_seconds()
    logger.info(' all workers job completed in %f seconds' % time_delta)
    
    global_properties, global_entities = {}, {}
    for worker_id in range(1, worker_count+1):
        new_entities, new_properties = load_worker_data(logger, global_worker_config[worker_id])
        global_properties.update(new_properties)
        global_entities.update(new_entities)

    logger.info(' total node infromation extracted for %d/%d entities, %d/%d properties' % (len(global_entities), len(entities), len(global_properties), len(properties)))
    
    #save the files
    file_path = os.path.join(os.path.abspath(config.get('store_path')), "{}-info.txt")
    
    properties_file = file_path.format("properties")    
    with open(properties_file, 'w') as data_file:
        json.dump(global_properties, data_file)
    logger.info(" successfully stored properties info to file : %s" % properties_file)
    
    entities_file = file_path.format("entities")    
    with open(entities_file, 'w') as data_file:
        json.dump(global_entities, data_file)
    logger.info(" stored entities info to file : %s" % entities_file)
    return global_entities, global_properties

if __name__ == "__main__":
    start_time = datetime.utcnow()
    log_file = os.path.abspath("/scratch/tabhishek/wikidata/next_process/logs/main.log")
    source_folder = os.path.abspath("/scratch/tabhishek/wikidata/data")
    store_path = os.path.abspath("/scratch/tabhishek/wikidata/next_process/data")
    target_nodes_file = os.path.abspath("/home/tushar.abhishek/ire/research/wikidata/source_nodes.txt")
    
    logger = ManualLogger('main', log_file, use_stdout=True)

    target_nodes = []
    logger.info('loading source nodes from file : %s' %(os.path.abspath(target_nodes_file)))
    with open(os.path.abspath(target_nodes_file), 'r') as source_file:
        for line in source_file.readlines():
            entity_id = clean_str(line)
            target_nodes.append(entity_id)

    properties_file = os.path.join(source_folder, 'properties.txt')
    entities_file = os.path.join(source_folder, 'entities.txt')
    triples_file = os.path.join(source_folder, 'triples.txt')

    entities = load_data(logger, entities_file)
    properties = load_data(logger, properties_file)
    triples = load_data(logger, triples_file)
    
    config = {'thread_count': 2,
                'marker': 1e6,
                'wikidata_dump_path': "/scratch/tabhishek/wikidata/latest-all.json.bz2",
                'log_path': "/scratch/tabhishek/wikidata/next_process/logs",
                'store_path': store_path,
                'target_nodes': target_nodes,
    }

    entities_info, properties_info = extract_node_data_from_dump(logger, config, properties, entities)

    mapping_file_path = os.path.join(os.path.abspath(store_path), "{}-map.txt")
    
    entities_to_id = {}
    sorted_entities = sorted([i for i in entities_info])
    id_counter = 1 
    with open(mapping_file_path.format('entities'), 'w') as map_file:
        for entity in sorted_entities:
            entity = clean_str(entity)
            if len(entity)==0 or entity=='':
                continue
            entities_to_id[entity] = id_counter
            map_file.write("%d %s\n"%(id_counter, entity))
            id_counter+=1
    logger.info('successfully created the entities map file')

    properties_to_id = {}
    sorted_properties = sorted([i for i in properties_info])
    id_counter = 1
    with open(mapping_file_path.format('properties'), 'w') as map_file:
        for prop in sorted_properties:
            prop = clean_str(prop)
            if len(prop)==0 or prop == '':
                continue
            properties_to_id[prop] = id_counter
            map_file.write("%d %s\n"%(id_counter, prop))
            id_counter+=1
    logger.info('successfully created the properties map file')

    processed_triples_file = os.path.join(os.path.abspath(store_path), "coded-triples.txt")
    
    invalid_triple_format, null_triplet, empty_mapping = 0, 0, 0
    with open(processed_triples_file, 'w') as coded_file:
        for triplet in triples:
            data = triplet.split(' ')
            if len(data) != 3:
                invalid_triple_format +=1
                continue
            head, prop, tail = clean_str(data[0]), clean_str(data[1]), clean_str(data[2])
            if len(head)==0 or len(prop)==0 or len(tail)==0 or head=='' or prop=='' or tail=='':
                null_triplet+=1
                continue
            head_id, prop_id, tail_id = entities_to_id.get(head, None), properties_to_id.get(prop, None), entities_to_id.get(tail, None)
            if head_id is None or prop_id is None or tail_id is None:
                empty_mapping+=1
                continue
            coded_file.write('%s %s %s\n'% (head_id, prop_id, tail_id))
    
    loss = invalid_triple_format + null_triplet + empty_mapping
    logger.info(' total processed triples %d out of %d original triples.' % (len(triples) - loss, len(triples)))
    logger.info(' invalid-format : %d, null_triples : %d, empty_mapping : %d.' % (invalid_triple_format, null_triplet, empty_mapping))
    
    time_delta = (datetime.utcnow() - start_time).total_seconds()
    logger.info("complete the whole process in %s" % time_delta)

    
