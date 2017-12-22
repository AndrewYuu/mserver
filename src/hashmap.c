#include "utils.h"
#include "debug.h"
#include <errno.h>
#include <string.h>

#define MAP_KEY(base, len) (map_key_t) {.key_base = base, .key_len = len}
#define MAP_VAL(base, len) (map_val_t) {.val_base = base, .val_len = len}
#define MAP_NODE(key_arg, val_arg, tombstone_arg) (map_node_t) {.key = key_arg, .val = val_arg, .tombstone = tombstone_arg}

hashmap_t *create_map(uint32_t capacity, hash_func_f hash_function, destructor_f destroy_function) {
    if(capacity <= 0 || hash_function == NULL || destroy_function == NULL){
        errno = EINVAL;
        return NULL;
    }

    hashmap_t *hashmap = calloc(1, sizeof(hashmap_t));
    hashmap->capacity = capacity;
    hashmap->nodes = calloc(capacity, sizeof(map_node_t));
    hashmap->hash_function = hash_function;
    hashmap->destroy_function = destroy_function;
    if(pthread_mutex_init(&hashmap->write_lock, NULL) != 0){
        errno = EINVAL;
        exit(1);
    }
    if(pthread_mutex_init(&hashmap->fields_lock, NULL) != 0){
        errno = EINVAL;
        exit(1);
    }
    return hashmap;
}


bool put(hashmap_t *self, map_key_t key, map_val_t val, bool force) {
    //IF ANY PARAMETERS INVALID
    if(self == NULL || self->invalid || key.key_base == NULL || val.val_base == NULL){
        errno = EINVAL;
        return false;
    }

    debug("Put function force value: %d", force);

    //THREAD TAKES THE LOCK. NO OTHER THREAD CAN COME IN WHEN WRITING.
    pthread_mutex_lock(&self->write_lock);

    //IF MAP IS FULL AND FORCE IS FALSE
    if(self->size == self->capacity && force == 0){
        errno = ENOMEM;
        pthread_mutex_unlock(&self->write_lock);
        return false;
    }

    uint32_t index = get_index(self, key);
    //IF THE MAP IS FULL AND FORCE IS TRUE.
    if(self->size == self->capacity && force == 1){
        debug("Put when map is full and force is true.");
        int total_count = 0;
        //FIRST, SEARCH IF THERE IS AN EXISTING KEY FOR IT. IF THERE IS, REPLACE THAT KEY. OTHERWISE, HASH IT IMMEDIATELY TO ITS INDEX.
        while(total_count < self->capacity){
            //COMPARE KEYS OF THE SAME LENGTH
            if(key.key_len == self->nodes[index].key.key_len){
                //IF THEY ARE THE SAME KEY, SIMPLY REPLACE THE VALUE FOR THAT KEY.
                if(memcmp(key.key_base, self->nodes[index].key.key_base, key.key_len) == 0){
                    //DESTROY FUNCTION ON KEY AND VAL. destroy_function() DOES NOT FREE THE NODE. WILL ONLY FREE THE KEY AND VAL.

                    debug("Put into hashmap at same key.");
                    self->destroy_function(self->nodes[index].key, self->nodes[index].val);
                    self->nodes[index].key = key;
                    self->nodes[index].val = val;

                    pthread_mutex_unlock(&self->write_lock);

                    return true;
                }
            }
            index = (index + 1) % self->capacity;
            total_count++;
        }
        debug("There is no same key in the full hashmap. Just replace at hashed index");
        //THERE IS NO SAME KEY IN THE HASHMAP. JUST REPLACE AT THE HASHED INDEX.
        index = get_index(self, key);
        //DESTROY FUNCTION ON KEY AND VAL. USER's destroy_function() DOES NOT FREE THE NODE. ONLY THE KEY AND VAL.
        self->destroy_function(self->nodes[index].key, self->nodes[index].val);
        self->nodes[index].key = key;
        self->nodes[index].val = val;

        pthread_mutex_unlock(&self->write_lock);

        return true;
    }
    //OTHERWISE, THE MAP IS NOT FULL AND FORCE CAN BE TRUE OR NOT TRUE
    else{
        debug("Map is not full");
        int total_count = 0;
        //FIRST FIND IF THERE IS A NODE WITH THE SAME KEY. IF THERE IS ONE, REPLACE THAT NODE'S VALUE.
        //SKIP OVER TOMBSTONES BECAUSE THERE CAN BE MORE NODES AFTER THE TOMBSTONE TO CHECK.
        while((self->nodes[index].key.key_base != 0 && self->nodes[index].key.key_len != 0
        && self->nodes[index].val.val_base != 0 && self->nodes[index].val.val_len != 0)
        || self->nodes[index].tombstone == 1){
            if(total_count == self->capacity){
                break;
            }
            //COMPARE KEYS OF THE SAME LENGTH
            if(key.key_len == self->nodes[index].key.key_len){
                //IF THEY ARE THE SAME KEY, SIMPLY REPLACE THE VALUE FOR THAT KEY.
                if(memcmp(key.key_base, self->nodes[index].key.key_base, key.key_len) == 0){
                    debug("There exists a same key. Destroy the node and replace key and value.");
                    //DESTROY FUNCTION ON KEY AND VAL. destroy_function() DOES NOT FREE THE NODE. WILL ONLY FREE THE KEY AND VAL.
                    self->destroy_function(self->nodes[index].key, self->nodes[index].val);
                    self->nodes[index].key = key;
                    self->nodes[index].val = val;

                    pthread_mutex_unlock(&self->write_lock);

                    return true;
                }
            }
            index = (index + 1) % self->capacity;
            total_count++;
        }
        //THERE IS NO NODE WITH THE SAME KEY.
        debug("There is no node with the same key.");
        //WHEN THE MAP IS NOT FULL, LINEAR PROBE IF NEEDED
        //IF THE INDEX ALREADY HAS AN ELEMENT LINEAR PROBE TO FIND THE FIRST NEXT FREE INDEX OR A TOMBSTONE AND PUT
        total_count = 0;
        index = get_index(self, key);
        while((self->nodes[index].key.key_base != 0 && self->nodes[index].key.key_len != 0
        && self->nodes[index].val.val_base != 0 && self->nodes[index].val.val_len != 0)
            || self->nodes[index].tombstone == 1){
            //MAP IS FULL
            if(total_count == self->capacity){
                pthread_mutex_unlock(&self->write_lock);
                return false;
            }
            index = (index + 1) % self->capacity;
            total_count++;
        }

        //CURRENT NODE IN THE ARRAY IS FREE.
        if((self->nodes[index].key.key_base == 0 && self->nodes[index].key.key_len == 0
        && self->nodes[index].val.val_base == 0 && self->nodes[index].val.val_len == 0)
            || self->nodes[index].tombstone == 1){
            debug("Current node in the array is free. Put into this node.");
            debug("key value: %d", *(int *)key.key_base);
            self->nodes[index].key = key;
            self->nodes[index].val = val;
            self->nodes[index].tombstone = 0;
            self->size = (self->size) + 1;
        }

        pthread_mutex_unlock(&self->write_lock);

        return true;
    }
}

map_val_t get(hashmap_t *self, map_key_t key) {
    //WHEN SEARCHING, SKIP OVER TOMBSTONED NODES. ONCE A NODE IS REACHED THAT IS EMPTY, AND
    //KEY HAS YET TO BE FOUND, THE KEY VALUE PAIR DOES NOT EXIST.

    if(self == NULL || self->invalid){
        errno = EINVAL;
        return MAP_VAL(NULL, 0);
    }

    // take fields lock
    // increment num readers
    // if you're the first reader, take write lock
    // put back fields lock


    // read

    // take fields
    // decrement num readers
    // if you're the last reader to leave, put back write lock
    // put back fields lock

    //fields_lock TO PROTECT ACCESS TO THE READER COUNTER. MULTIPLE THREADS CAN CORRUPT THE num_readers COUNTER.
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers = (self->num_readers) + 1;
    if(self->num_readers == 1){
        //ONLY THE FIRST READER TAKES THE WRITE LOCK. THIS IS BECAUSE IF EVERY READER TAKES A WRITE LOCK,
        //THE HASH MAP WOULD BE SINGLE THREADED AS EVERY READER AND WRITER WOULD WAIT UNTIL THE WRITE LOCK IS UNLOCKED.
        //THATS WHY EVERY SUBSEQUENT READER AFTER THE FIRST NEED NOT TO TAKE A WRITE LOCK, SO THAT THERE CAN BE AN UNLIMITED
        //NUMBER OF READERS, BUT NO WRITERS. ONCE THIS READER THREAD (THREAD THAT RUNS get() METHOD), WHEN A WRITER
        //THREAD (THREAD THAT RUNS put() OR delete() METHODS), THOSE THREADS NEED TO WAIT UNTIL THE LAST READER RELEASES THE WRITE LOCK.
        //THIS PREVENTS THREADS TO WRITE TO THE DATASTRUCTURE WHEN THREAD(S) ARE READING IT.
        pthread_mutex_lock(&self->write_lock);
    }
    pthread_mutex_unlock(&self->fields_lock);

    uint32_t index = get_index(self, key);
    int total_count = 0;

    while(total_count < self->capacity){
        if(self->nodes[index].tombstone == 1){
            index = (index + 1) % self->capacity;
            total_count++;
        }
        if(key.key_len == self->nodes[index].key.key_len){
            if(memcmp(key.key_base, self->nodes[index].key.key_base, key.key_len) == 0){

                debug("KEY VALUE PAIR FOUND.");
                map_val_t returnval = MAP_VAL(self->nodes[index].val.val_base, self->nodes[index].val.val_len);

                pthread_mutex_lock(&self->fields_lock);
                self->num_readers = (self->num_readers) - 1;
                if(self->num_readers == 0){
                    pthread_mutex_unlock(&self->write_lock);
                }
                pthread_mutex_unlock(&self->fields_lock);

                return returnval;
            }
        }
        //EMPTY NODE IN THE ARRAY. ONCE REACH EMPTY NODE, ASSUME THAT THE KEY DOESNT EXIST.
        if(self->nodes[index].key.key_base == 0 && self->nodes[index].key.key_len == 0 && self->nodes[index].tombstone == 0){
            //IF KEY IS NOT FOUND IN THE MAP, RETURN THIS.
            debug("KEY VALUE PAIR NOT FOUND. DOES NOT EXIST BECAUSE EMPTY NODE FOUND.");
            pthread_mutex_lock(&self->fields_lock);
            self->num_readers = (self->num_readers) - 1;
            if(self->num_readers == 0){
                pthread_mutex_unlock(&self->write_lock);
            }
            pthread_mutex_unlock(&self->fields_lock);

            return MAP_VAL(NULL, 0);
        }
        else{
            index = (index + 1) % self->capacity;
            total_count++;
        }
    }
    debug("KEY VALUE PAIR NOT FOUND. ENTIRE MAP SEARCHED.");

    //IF KEY IS NOT FOUND IN THE ARRAY AND ITS BEEN COMPLETELY SEARCHED.
    pthread_mutex_lock(&self->fields_lock);
    self->num_readers = (self->num_readers) - 1;
    if(self->num_readers == 0){
        pthread_mutex_unlock(&self->write_lock);
    }
    pthread_mutex_unlock(&self->fields_lock);

    return MAP_VAL(NULL, 0);
}

map_node_t delete(hashmap_t *self, map_key_t key) {

    pthread_mutex_lock(&self->write_lock);

    uint32_t index = get_index(self, key);

    //CHECK IF IMMEDIATE INDEX CONTAINS THE KEY
    if(key.key_len == self->nodes[index].key.key_len){
        //IF THEY ARE THE SAME KEY, SIMPLY REPLACE THE VALUE FOR THAT KEY.
        if(memcmp(key.key_base, self->nodes[index].key.key_base, key.key_len) == 0){
            //REMOVE THE NODE
            map_node_t returnNode = MAP_NODE(key, self->nodes[index].val, self->nodes[index].tombstone);
            self->nodes[index].key.key_base = 0;
            self->nodes[index].key.key_len = 0;
            self->nodes[index].val.val_base = 0;
            self->nodes[index].val.val_len = 0;
            self->nodes[index].tombstone = 1;
            self->size = (self->size) - 1;

            pthread_mutex_unlock(&self->write_lock);

            return returnNode;
        }
    }

    //IMMEDIATE INDEX DOES NOT CONTAIN THE KEY, SO LOOP THROUGH THE REST OF THE HASH MAP TO FIND IT.
    int total_count = 0;
    while(total_count < self->capacity){
        if(self->nodes[index].tombstone == 1){
            index = (index + 1) % self->capacity;
            total_count++;
        }
        if(key.key_len == self->nodes[index].key.key_len){
            if(memcmp(key.key_base, self->nodes[index].key.key_base, key.key_len) == 0){
                //REMOVE THE NODE
                map_node_t returnNode = MAP_NODE(key, self->nodes[index].val, self->nodes[index].tombstone);
                self->nodes[index].key.key_base = 0;
                self->nodes[index].key.key_len = 0;
                self->nodes[index].val.val_base = 0;
                self->nodes[index].val.val_len = 0;
                self->nodes[index].tombstone = 1;
                self->size = (self->size) - 1;

                pthread_mutex_unlock(&self->write_lock);

                return returnNode;
            }
        }
        //EMPTY NODE IN THE ARRAY. ONCE REACH EMPTY NODE, ASSUME THAT THE KEY DOESNT EXIST.
        if(self->nodes[index].key.key_base == 0 && self->nodes[index].key.key_len == 0 && self->nodes[index].tombstone == 0){
            //IF KEY IS NOT FOUND.

            pthread_mutex_unlock(&self->write_lock);

            return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
        }
        else{
            index = (index + 1) % self->capacity;
            total_count++;
        }
    }

    //KEY IS NOT FOUND AFTER SEARCHING ENTIRE ARRAY

    pthread_mutex_unlock(&self->write_lock);

    return MAP_NODE(MAP_KEY(NULL, 0), MAP_VAL(NULL, 0), false);
}

bool clear_map(hashmap_t *self) {

    if(self == NULL){
        errno = EINVAL;
        return false;
    }

    pthread_mutex_lock(&self->write_lock);

    int index = 0;
    int total_count = 0;

    //WHEN CAN OPERATION FAIL?????
    while(total_count < self->capacity){

        if((self->nodes[index].key.key_base != 0 && self->nodes[index].key.key_len != 0
            && self->nodes[index].val.val_base != 0 && self->nodes[index].val.val_len != 0)
            || self->nodes[index].tombstone == 1){

            self->nodes[index].key.key_base = 0;
            self->nodes[index].key.key_len = 0;
            self->nodes[index].val.val_base = 0;
            self->nodes[index].val.val_len = 0;
            self->nodes[index].tombstone = 0;
            self->destroy_function(MAP_KEY(self->nodes[index].key.key_base, self->nodes[index].key.key_len), MAP_VAL(self->nodes[index].val.val_base, self->nodes[index].val.val_len));
        }

        index = (index + 1) % self->capacity;
        total_count++;
    }

    self->size = 0;

    pthread_mutex_unlock(&self->write_lock);
    return true;
}

bool invalidate_map(hashmap_t *self) {

    if(self == NULL){
        errno = EINVAL;
        return false;
    }

    pthread_mutex_lock(&self->write_lock);

    int index = 0;
    int total_count = 0;

    //WHEN CAN OPERATION FAIL?????
    while(total_count < self->capacity){

        if((self->nodes[index].key.key_base != 0 && self->nodes[index].key.key_len != 0
            && self->nodes[index].val.val_base != 0 && self->nodes[index].val.val_len != 0)
            || self->nodes[index].tombstone == 1){

            self->nodes[index].key.key_base = 0;
            self->nodes[index].key.key_len = 0;
            self->nodes[index].val.val_base = 0;
            self->nodes[index].val.val_len = 0;
            self->nodes[index].tombstone = 0;
            self->destroy_function(MAP_KEY(self->nodes[index].key.key_base, self->nodes[index].key.key_len), MAP_VAL(self->nodes[index].val.val_base, self->nodes[index].val.val_len));
        }

        index = (index + 1) % self->capacity;
        total_count++;
    }

    self->size = 0;

    free(self->nodes);
    self->invalid = true;

    pthread_mutex_unlock(&self->write_lock);
    return true;
}
