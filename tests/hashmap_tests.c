#include <criterion/criterion.h>
#include <criterion/logging.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>

#include "hashmap.h"
#include "utils.h"
#include "debug.h"
// #define NUM_THREADS 1500 //TEST 9 ONLY
//#define NUM_THREADS 1000 (all tests but test 9)
#define NUM_THREADS 15 //TEST 11


hashmap_t *global_map;

typedef struct map_insert_t {
    void *key_ptr;
    void *val_ptr;
} map_insert_t;

/* Used in item destruction */
void map_free_function(map_key_t key, map_val_t val) {
    free(key.key_base);
    free(val.val_base);
}

uint32_t jenkins_hash(map_key_t map_key) {
    const uint8_t *key = map_key.key_base;
    size_t length = map_key.key_len;
    size_t i = 0;
    uint32_t hash = 0;

    while (i != length) {
        hash += key[i++];
        hash += hash << 10;
        hash ^= hash >> 6;
    }

    hash += hash << 3;
    hash ^= hash >> 11;
    hash += hash << 15;
    return hash;
}

void map_init(void) {
    global_map = create_map(NUM_THREADS, jenkins_hash, map_free_function);
}

void *thread_put(void *arg) {
    map_insert_t *insert = (map_insert_t *) arg;

    put(global_map, MAP_KEY(insert->key_ptr, sizeof(int)), MAP_VAL(insert->val_ptr, sizeof(int)), false);
    return NULL;
}

void *thread_get(void *arg) {
    map_insert_t *insert = (map_insert_t *) arg;
    int *exp = insert->val_ptr;
    map_val_t rv;
    //DELETE THREAD CAN DELETE BEFORE get IS CALLED WHEN CONTEXT SWITCHED HERE.
    rv = get(global_map, MAP_KEY(insert->key_ptr, sizeof(int)));
    // cr_assert_not_null(rv.val_base);
    // assert expected val
    if(rv.val_base != NULL){
        debug("%p, %d", exp, *exp);
        cr_assert_eq(*exp, *(int *)rv.val_base, "Not equals. Value expected: %d, Value got: %d", *exp, rv.val_base);
    }
    return NULL;
}

void *thread_delete(void *arg) {
    map_insert_t *insert = (map_insert_t *) arg;

    delete(global_map, MAP_KEY(insert->key_ptr, sizeof(int)));
    return NULL;
}

void map_fini(void){
    invalidate_map(global_map);
}

void assert_equal_nodes(map_node_t node, map_key_t key, map_val_t val){
    cr_assert_eq(node.key.key_base, key.key_base, "Key base are not equal");
    cr_assert_eq(node.key.key_len, key.key_len, "Keys length are not equal");
    cr_assert_eq(node.val.val_base, val.val_base, "Val base are not equal");
    cr_assert_eq(node.val.val_len, val.val_len, "Val length are not equal");
}

Test(map_suite, 00_creation, .timeout = 2, .init = map_init, .fini = map_fini) {
    cr_assert_not_null(global_map, "Map returned was NULL");
}

Test(map_suite, 01_single_put, .timeout = 2, .init = map_init, .fini = map_fini){
    int *key_ptr = malloc(sizeof(int));
    int *val_ptr = malloc(sizeof(int));
    *key_ptr = 5;
    *val_ptr = 10 * 2;

    map_insert_t *insert = malloc(sizeof(map_insert_t));
    insert->key_ptr = key_ptr;
    insert->val_ptr = val_ptr;

    map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
    map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

    put(global_map, key, val, false);
    map_val_t getval = get(global_map, key);
    cr_assert_eq(*val_ptr, *(int *)getval.val_base, "Not equals. Value expected: %d, Value got: %d", *val_ptr, *(int *)getval.val_base);
    // cr_assert_eq(node.key.key_base, key.key_base, "Key base are not equal");
    // cr_assert_eq(node.key.key_len, sizeof(int), "Keys length are not equal");
    // cr_assert_eq(node.val.val_base, val.val_base, "Val base are not equal");
    // cr_assert_eq(node.val.val_len, sizeof(int), "Val length are not equal");
}

Test(map_suite, 03_single_multiput, .timeout = 2, .init = map_init, .fini = map_fini){
    //AT INDEX 10, KEY GETS HASHED TO A FILLED INDEX, SO ASSERT EQUAL AT THE INDEX FOR EXPECTED KEY AT 10 IS NOT THE SAME.
    for(int index = 0; index < 15; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
        map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

        put(global_map, key, val, false);
        map_val_t getval = get(global_map, key);
        cr_assert_eq(*val_ptr, *(int *)getval.val_base, "Not equals. Value expected: %d, Value got: %d", *val_ptr, *(int *)getval.val_base);
    }
}

Test(map_suite, 04_single_multiput_2, .timeout = 2, .init = map_init, .fini = map_fini){
    //AT INDEX 10, KEY GETS HASHED TO A FILLED INDEX, SO ASSERT EQUAL AT THE INDEX FOR EXPECTED KEY AT 10 IS NOT THE SAME.
    int num_items = 0;
    for(int index = 0; index < 15; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
        map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

        put(global_map, key, val, false);
        num_items = global_map->size;
    }
    cr_assert_eq(num_items, 15, "Had %d items in map. Expected %d", num_items, 15);
}

Test(map_suite, 05_single_delete, .timeout = 2, .init = map_init, .fini = map_fini){
    int num_items = 0;

    int *key_ptr = malloc(sizeof(int));
    int *val_ptr = malloc(sizeof(int));
    *key_ptr = 5;
    *val_ptr = 5 * 2;

    map_insert_t *insert = malloc(sizeof(map_insert_t));
    insert->key_ptr = key_ptr;
    insert->val_ptr = val_ptr;

    map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
    map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));
    put(global_map, key, val, false);

    int *key_ptr_2 = malloc(sizeof(int));
    int *val_ptr_2 = malloc(sizeof(int));
    *key_ptr_2 = 6;
    *val_ptr_2 = 6 * 2;

    map_insert_t *insert_2 = malloc(sizeof(map_insert_t));
    insert_2->key_ptr = key_ptr_2;
    insert_2->val_ptr = val_ptr_2;

    map_key_t key_2 = MAP_KEY(insert_2->key_ptr, sizeof(int));
    map_val_t val_2 = MAP_VAL(insert_2->val_ptr, sizeof(int));
    put(global_map, key_2, val_2, false);

    int *key_ptr_3 = malloc(sizeof(int));
    int *val_ptr_3 = malloc(sizeof(int));
    *key_ptr_3 = 7;
    *val_ptr_3 = 7 * 2;

    map_insert_t *insert_3 = malloc(sizeof(map_insert_t));
    insert_3->key_ptr = key_ptr_3;
    insert_3->val_ptr = val_ptr_3;

    map_key_t key_3 = MAP_KEY(insert_3->key_ptr, sizeof(int));
    map_val_t val_3 = MAP_VAL(insert_3->val_ptr, sizeof(int));
    put(global_map, key_3, val_3, false);

    num_items = global_map->size;
    cr_assert_eq(num_items, 3, "Had %d items in map. Expected %d", num_items, 3);

    delete(global_map, key_2);

    num_items = global_map->size;
    cr_assert_eq(num_items, 2, "Had %d items in map. Expected %d", num_items, 2);

    delete(global_map, key);

    num_items = global_map->size;
    cr_assert_eq(num_items, 1, "Had %d items in map. Expected %d", num_items, 1);

    delete(global_map, key_3);

    num_items = global_map->size;
    cr_assert_eq(num_items, 0, "Had %d items in map. Expected %d", num_items, 0);

}

Test(map_suite, 06_single_get_with_tombstone, .timeout = 2, .init = map_init, .fini = map_fini){
    int num_items = 0;

    int *key_ptr = malloc(sizeof(int));
    int *val_ptr = malloc(sizeof(int));
    *key_ptr = 5;
    *val_ptr = 5 * 2;

    map_insert_t *insert = malloc(sizeof(map_insert_t));
    insert->key_ptr = key_ptr;
    insert->val_ptr = val_ptr;

    map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
    map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));
    put(global_map, key, val, false);

    int *key_ptr_2 = malloc(sizeof(int));
    int *val_ptr_2 = malloc(sizeof(int));
    *key_ptr_2 = 6;
    *val_ptr_2 = 6 * 2;

    map_insert_t *insert_2 = malloc(sizeof(map_insert_t));
    insert_2->key_ptr = key_ptr_2;
    insert_2->val_ptr = val_ptr_2;

    map_key_t key_2 = MAP_KEY(insert_2->key_ptr, sizeof(int));
    map_val_t val_2 = MAP_VAL(insert_2->val_ptr, sizeof(int));
    put(global_map, key_2, val_2, false);

    int *key_ptr_3 = malloc(sizeof(int));
    int *val_ptr_3 = malloc(sizeof(int));
    *key_ptr_3 = 7;
    *val_ptr_3 = 7 * 2;

    map_insert_t *insert_3 = malloc(sizeof(map_insert_t));
    insert_3->key_ptr = key_ptr_3;
    insert_3->val_ptr = val_ptr_3;

    map_key_t key_3 = MAP_KEY(insert_3->key_ptr, sizeof(int));
    map_val_t val_3 = MAP_VAL(insert_3->val_ptr, sizeof(int));
    put(global_map, key_3, val_3, false);

    num_items = global_map->size;
    cr_assert_eq(num_items, 3, "Had %d items in map. Expected %d", num_items, 3);

    delete(global_map, key_2);

    num_items = global_map->size;
    cr_assert_eq(num_items, 2, "Had %d items in map. Expected %d", num_items, 2);

    map_val_t get_value = get(global_map, key_3);
    cr_assert_eq(*(int *)get_value.val_base, 14, "Value is not expected. Is %d, expected %d", *(int *)get_value.val_base, 14);
}


Test(map_suite, 02_multithreaded, .timeout = 2, .init = map_init, .fini = map_fini) {
    pthread_t thread_ids[NUM_THREADS];

    // spawn NUM_THREADS threads to put elements
    for(int index = 0; index < NUM_THREADS; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before checking queue
    for(int index = 0; index < NUM_THREADS; index++) {
        pthread_join(thread_ids[index], NULL);
    }

    int num_items = global_map->size;
    cr_assert_eq(num_items, NUM_THREADS, "Had %d items in map. Expected %d", num_items, NUM_THREADS);
}

Test(map_suite, 07_multithreaded_2, .timeout = 2, .init = map_init, .fini = map_fini) {
    pthread_t thread_ids[NUM_THREADS];

    // spawn NUM_THREADS (15) threads to put elements
    for(int index = 0; index < NUM_THREADS; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }

    pthread_t thread_ids_2[10];
    // spawn 10 threads to get elements
    for(int index = 0; index < 10; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids_2[index], NULL, thread_get, insert) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before continuing
    for(int index = 0; index < NUM_THREADS; index++) {
        pthread_join(thread_ids[index], NULL);
    }


    pthread_t thread_ids_3[7];
    // spawn 7 threads to delete elements (key values 0 - 7 removed)
    for(int index = 0; index < 7; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids_3[index], NULL, thread_delete, insert) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before continuing
    for(int index = 0; index < 7; index++) {
        pthread_join(thread_ids_3[index], NULL);
    }

    pthread_t thread_ids_4[5];
    // spawn 5 threads to get elements
    for(int index = 0; index < 5; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids_4[index], NULL, thread_get, insert) != 0)
            exit(EXIT_FAILURE);
    }

    pthread_t thread_ids_5[3];
    // spawn 3 threads to put elements (key values 0 - 3 added)
    for(int index = 0; index < 3; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids_5[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before continuing
    for(int index = 0; index < 3; index++) {
        pthread_join(thread_ids_5[index], NULL);
    }

    int num_items = global_map->size;
    cr_assert_eq(num_items, 11, "Had %d items in map. Expected %d", num_items, 11);
}

Test(map_suite, 08_single_put_samekey, .timeout = 2, .init = map_init, .fini = map_fini){
    int num_items = 0;

    int *key_ptr = malloc(sizeof(int));
    int *val_ptr = malloc(sizeof(int));
    *key_ptr = 5;
    *val_ptr = 5 * 2;

    map_insert_t *insert = malloc(sizeof(map_insert_t));
    insert->key_ptr = key_ptr;
    insert->val_ptr = val_ptr;

    map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
    map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

    put(global_map, key, val, false);
    map_val_t get_value = get(global_map, key);
    cr_assert_eq(*(int *)get_value.val_base, 10, "Value is not expected. Is %d, expected %d", *(int *)get_value.val_base, 10);

    int *key_ptr_2 = malloc(sizeof(int));
    int *val_ptr_2 = malloc(sizeof(int));
    *key_ptr_2 = 5;
    *val_ptr_2 = 30 * 2;

    map_insert_t *insert_2 = malloc(sizeof(map_insert_t));
    insert_2->key_ptr = key_ptr_2;
    insert_2->val_ptr = val_ptr_2;

    map_key_t key_2 = MAP_KEY(insert_2->key_ptr, sizeof(int));
    map_val_t val_2 = MAP_VAL(insert_2->val_ptr, sizeof(int));
    put(global_map, key_2, val_2, false);
    map_val_t get_value_2 = get(global_map, key_2);
    cr_assert_eq(*(int *)get_value_2.val_base, 60, "Value is not expected. Is %d, expected %d", *(int *)get_value_2.val_base, 60);


    num_items = global_map->size;
    cr_assert_eq(num_items, 1, "Had %d items in map. Expected %d", num_items, 1);
}

Test(map_suite, 09_multithreaded_3, .timeout = 2, .init = map_init, .fini = map_fini) {
    pthread_t thread_ids[NUM_THREADS];

    // spawn 5 threads to put elements
    for(int index = 0; index < 5; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }

    // spawn 10 threads to put elements
    for(int index = 5; index < NUM_THREADS; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = 999;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }


    // wait for threads to die before checking queue
    for(int index = 0; index < NUM_THREADS; index++) {
        pthread_join(thread_ids[index], NULL);
    }

    int num_items = global_map->size;
    cr_assert_eq(num_items, 15, "Had %d items in map. Expected %d", num_items, 15);

    int count = 0;

    for(int index = 0; index < NUM_THREADS; index++){
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        map_val_t get_value = get(global_map, MAP_KEY(insert->key_ptr, sizeof(int)));
        if(*(int *)get_value.val_base == 999){
            count++;
        }
    }
    cr_assert_eq(count, 10, "Had %d 999 values. Expected %d", count, 10);
}


//THE FOLLOWING TEST SHOULD FAIL
Test(map_suite, 10_multithreaded_randoms, .timeout = 2, .init = map_init, .fini = map_fini) {
    pthread_t thread_ids[NUM_THREADS];

    // spawn NUM_THREADS (15) threads to put elements
    for(int index = 0; index < NUM_THREADS; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }

    pthread_t thread_ids_2[5];
    // spawn 5 threads to put elements
    for(int index = 0; index < 5; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = 999;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        if(pthread_create(&thread_ids_2[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }


    // wait for threads to die before checking queue
    for(int index = 0; index < NUM_THREADS; index++) {
        pthread_join(thread_ids[index], NULL);
    }
    // wait for threads to die before checking queue
    for(int index = 0; index < 5; index++) {
        pthread_join(thread_ids_2[index], NULL);
    }

    int num_items = global_map->size;
    cr_assert_eq(num_items, 15, "Had %d items in map. Expected %d", num_items, 15);

    int count = 0;

    for(int index = 0; index < NUM_THREADS; index++){
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        map_val_t get_value = get(global_map, MAP_KEY(insert->key_ptr, sizeof(int)));
        if(*(int *)get_value.val_base == 999){
            count++;
        }
    }
    cr_assert_eq(count, 5, "Had %d 999 values. Expected %d", count, 5);
}


Test(map_suite, 11_multithreaded_get_delete, .timeout = 2, .init = map_init, .fini = map_fini) {
     pthread_t thread_ids[15];
     map_insert_t *vals[15];

    // spawn NUM_THREADS 15 threads to put elements
    for(int index = 0; index < 15; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;
        vals[index] = insert;

        if(pthread_create(&thread_ids[index], NULL, thread_put, insert) != 0)
            exit(EXIT_FAILURE);
    }

    for(int index = 0; index < 15; index++) {
        pthread_join(thread_ids[index], NULL);
    }

    pthread_t thread_ids_2[15];
    pthread_t thread_ids_3[5];

    for(int index = 0; index < 5; index++){
        //THREAD DELETE THE FIRST 5 KEYS
        if(pthread_create(&thread_ids_3[index], NULL, thread_delete, vals[index]) != 0)
            exit(EXIT_FAILURE);
    }

    for(int index = 5; index < 15; index++) {
        //THREAD GET THE FIFTH TO 15TH KEYS.
        if(pthread_create(&thread_ids_2[index], NULL, thread_get, vals[index]) != 0)
            exit(EXIT_FAILURE);
    }

    for(int index = 5; index < 15; index++) {
        pthread_join(thread_ids_2[index], NULL);
    }

    for(int index = 0; index < 5; index++) {
        pthread_join(thread_ids_3[index], NULL);
    }

    int num_items = global_map->size;
    cr_assert_eq(num_items, 10, "Had %d items in map. Expected %d", num_items, 10);
}



Test(map_suite, 12_single_put_samekey_delete, .timeout = 2, .init = map_init, .fini = map_fini){
    int num_items = 0;

    int *key_ptr = malloc(sizeof(int));
    int *val_ptr = malloc(sizeof(int));
    *key_ptr = 2;
    *val_ptr = 2 * 2;

    map_insert_t *insert = malloc(sizeof(map_insert_t));
    insert->key_ptr = key_ptr;
    insert->val_ptr = val_ptr;

    map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
    map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

    put(global_map, key, val, false);
    map_val_t get_value = get(global_map, key);
    cr_assert_eq(*(int *)get_value.val_base, 4, "Value is not expected. Is %d, expected %d", *(int *)get_value.val_base, 4);


    int *key_ptr_3 = malloc(sizeof(int));
    int *val_ptr_3 = malloc(sizeof(int));
    *key_ptr_3 = 3;
    *val_ptr_3 = 3 * 2;

    map_insert_t *insert_3 = malloc(sizeof(map_insert_t));
    insert_3->key_ptr = key_ptr_3;
    insert_3->val_ptr = val_ptr_3;

    map_key_t key_3 = MAP_KEY(insert_3->key_ptr, sizeof(int));
    map_val_t val_3 = MAP_VAL(insert_3->val_ptr, sizeof(int));

    put(global_map, key_3, val_3, false);
    map_val_t get_value_3 = get(global_map, key_3);
    cr_assert_eq(*(int *)get_value_3.val_base, 6, "Value is not expected. Is %d, expected %d", *(int *)get_value_3.val_base, 6);

    delete(global_map, key);

    int *key_ptr_2 = malloc(sizeof(int));
    int *val_ptr_2 = malloc(sizeof(int));
    *key_ptr_2 = 3;
    *val_ptr_2 = 30 * 2;

    map_insert_t *insert_2 = malloc(sizeof(map_insert_t));
    insert_2->key_ptr = key_ptr_2;
    insert_2->val_ptr = val_ptr_2;

    map_key_t key_2 = MAP_KEY(insert_2->key_ptr, sizeof(int));
    map_val_t val_2 = MAP_VAL(insert_2->val_ptr, sizeof(int));
    put(global_map, key_2, val_2, false);
    map_val_t get_value_2 = get(global_map, key_2);
    cr_assert_eq(*(int *)get_value_2.val_base, 60, "Value is not expected. Is %d, expected %d", *(int *)get_value_2.val_base, 60);


    num_items = global_map->size;
    cr_assert_eq(num_items, 1, "Had %d items in map. Expected %d", num_items, 1);
}

Test(map_suite, 13_single_forceput, .timeout = 2, .init = map_init, .fini = map_fini){
    int num_items = 0;

    for(int index = 0; index < 15; index++) {
        int *key_ptr = malloc(sizeof(int));
        int *val_ptr = malloc(sizeof(int));
        *key_ptr = index;
        *val_ptr = index * 2;

        map_insert_t *insert = malloc(sizeof(map_insert_t));
        insert->key_ptr = key_ptr;
        insert->val_ptr = val_ptr;

        map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
        map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

        put(global_map, key, val, false);
        num_items = global_map->size;
    }
    cr_assert_eq(num_items, 15, "Had %d items in map. Expected %d", num_items, 15);

    int *key_ptr = malloc(sizeof(int));
    int *val_ptr = malloc(sizeof(int));
    *key_ptr = 2;
    *val_ptr = 100 * 2;

    map_insert_t *insert = malloc(sizeof(map_insert_t));
    insert->key_ptr = key_ptr;
    insert->val_ptr = val_ptr;

    map_key_t key = MAP_KEY(insert->key_ptr, sizeof(int));
    map_val_t val = MAP_VAL(insert->val_ptr, sizeof(int));

    put(global_map, key, val, true);
    map_val_t get_value = get(global_map, key);
    cr_assert_eq(*(int *)get_value.val_base, 200, "Value is not expected. Is %d, expected %d", *(int *)get_value.val_base, 200);


    int *key_ptr_3 = malloc(sizeof(int));
    int *val_ptr_3 = malloc(sizeof(int));
    *key_ptr_3 = 3;
    *val_ptr_3 = 10 * 2;

    map_insert_t *insert_3 = malloc(sizeof(map_insert_t));
    insert_3->key_ptr = key_ptr_3;
    insert_3->val_ptr = val_ptr_3;

    map_key_t key_3 = MAP_KEY(insert_3->key_ptr, sizeof(int));
    map_val_t val_3 = MAP_VAL(insert_3->val_ptr, sizeof(int));

    put(global_map, key_3, val_3, true);
    map_val_t get_value_3 = get(global_map, key_3);
    cr_assert_eq(*(int *)get_value_3.val_base, 20, "Value is not expected. Is %d, expected %d", *(int *)get_value_3.val_base, 20);
    get_value = get(global_map, key);
    cr_assert_eq(*(int *)get_value.val_base, 200, "Value is not expected. Is %d, expected %d", *(int *)get_value.val_base, 200);
    //IF WE DO NOT SEARCH FOR THE SAME KEY WHEN FORCE (IF WE SIMPLY PUT THE NEW KEY VALUE PAIR IMMEDIATELY
    //INTO THE INDEX WITHOUT CHECKING IF WE HAVE THE KEY ALREADY), SINCE KEY 3 HASHES TO THE SAME INDEX AS KEY 2,
    //KEY 2 WILL BE REPLACED BY THE NEW KEY 3, RESULTING IN TWO KEY 3s IN THE HASH TABLE AND
    //NO KEY 2, SO WHEN get() KEY 2 VALUE, IT WILL RETURN 0 SINCE IT DOESNT EXIST ANYMORE. THIS IS WRONG.
    //THERE SHOULD ONLY BE ONE INSTANCE OF EACH KEY VALUE.

    int *key_ptr_2 = malloc(sizeof(int));
    int *val_ptr_2 = malloc(sizeof(int));
    *key_ptr_2 = 3;
    *val_ptr_2 = 30 * 2;

    map_insert_t *insert_2 = malloc(sizeof(map_insert_t));
    insert_2->key_ptr = key_ptr_2;
    insert_2->val_ptr = val_ptr_2;

    map_key_t key_2 = MAP_KEY(insert_2->key_ptr, sizeof(int));
    map_val_t val_2 = MAP_VAL(insert_2->val_ptr, sizeof(int));
    put(global_map, key_2, val_2, true);
    map_val_t get_value_2 = get(global_map, key_2);
    cr_assert_eq(*(int *)get_value_2.val_base, 60, "Value is not expected. Is %d, expected %d", *(int *)get_value_2.val_base, 60);
}