#include <criterion/criterion.h>
#include <criterion/logging.h>
#include <unistd.h>
#include <errno.h>
#include <stdio.h>

#include "queue.h"
#define NUM_THREADS 100

queue_t *global_queue;

/* Used in item destruction */
void queue_free_function(void *item) {
    free(item);
}

void queue_init(void) {
    global_queue = create_queue();
}

void *thread_enqueue(void *arg) {
    enqueue(global_queue, arg);
    return NULL;
}

void *thread_dequeue() {
    dequeue(global_queue);
    return NULL;
}

void queue_fini(void) {
    invalidate_queue(global_queue, queue_free_function);
}

Test(queue_suite, 00_creation, .timeout = 2, .init = queue_init, .fini = queue_fini){
    cr_assert_not_null(global_queue, "Queue returned was null");
}

Test(queue_suite, 02_single_enqueue, .timeout = 2, .init = queue_init, .fini = queue_fini) {
    int *arg = malloc(sizeof(int));
    *arg = 5;
    enqueue(global_queue, arg);

    cr_assert_not_null(global_queue->front, "Front was null");
    cr_assert_eq(global_queue->front, global_queue->rear, "Front does not equal rear");
    cr_assert_eq(*((int *) global_queue->front->item), 5, "Item is not 5");
}

Test(queue_suite, 03_single_dequeue, .timeout = 2, .init = queue_init, .fini = queue_fini){
    int *arg = malloc(sizeof(int));
    *arg = 5;
    enqueue(global_queue, arg);
    dequeue(global_queue);
    cr_assert_eq(global_queue->front, global_queue->rear, "Front does not equal rear");
    cr_assert_null(global_queue->front, "Front was not null. Should be null.");
}

Test(queue_suite, 04_multi_dequeue, .timeout = 2, .init = queue_init, .fini = queue_fini){
    int *arg = malloc(sizeof(int));
    *arg = 5;
    int *arg2 = malloc(sizeof(int));
    *arg2 = 10;
    int *arg3 = malloc(sizeof(int));
    *arg3 = 15;
    int *arg4 = malloc(sizeof(int));
    *arg4 = 20;
    enqueue(global_queue, arg);
    enqueue(global_queue, arg2);
    enqueue(global_queue, arg3);
    enqueue(global_queue, arg4);
    cr_assert_eq(*((int *) global_queue->front->item), 5, "Front Item is not 5");
    dequeue(global_queue);
    cr_assert_eq(*((int *) global_queue->front->item), 10, "Front Item is not 10");
    cr_assert_eq(*((int *) global_queue->rear->item), 20, "Rear item is not 20");
    dequeue(global_queue);
    cr_assert_eq(*((int *) global_queue->front->item), 15, "Front Item is not 15");
    dequeue(global_queue);
    cr_assert_eq(*((int *) global_queue->front->item), 20, "Front Item is not 20");
    dequeue(global_queue);
    cr_assert_null(global_queue->front, "Front was not null. Should be null.");
    cr_assert_eq(global_queue->front, global_queue->rear, "Front does not equal rear");
}

Test(queue_suite, 01_multithreaded, .timeout = 2, .init = queue_init, .fini = queue_fini) {
    pthread_t thread_ids[NUM_THREADS];

    // spawn NUM_THREADS threads to enqueue elements
    for(int index = 0; index < NUM_THREADS; index++) {
        int *ptr = malloc(sizeof(int));
        *ptr = index;

        if(pthread_create(&thread_ids[index], NULL, thread_enqueue, ptr) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before checking queue
    for(int index = 0; index < NUM_THREADS; index++) {
        pthread_join(thread_ids[index], NULL);
    }

    // get number of items in queue
    int num_items;
    if(sem_getvalue(&global_queue->items, &num_items) != 0)
        exit(EXIT_FAILURE);

    cr_assert_eq(num_items, NUM_THREADS, "Had %d items. Expected: %d", num_items, NUM_THREADS);
}

Test(queue_suite, 05_multithreaded_2, .timeout = 2, .init = queue_init, .fini = queue_fini) {
    pthread_t thread_ids[1000];

    // spawn 1000 threads to enqueue elements
    for(int index = 0; index < 1000; index++) {
        int *ptr = malloc(sizeof(int));
        *ptr = index;

        if(pthread_create(&thread_ids[index], NULL, thread_enqueue, ptr) != 0)
            exit(EXIT_FAILURE);
    }

    pthread_t thread_ids_2[250];
    // spawn 250 threads to dequeue elements
    for(int index = 0; index < 250; index++) {
        if(pthread_create(&thread_ids_2[index], NULL, thread_dequeue, NULL) != 0)
            exit(EXIT_FAILURE);
    }

    pthread_t thread_ids_3[400];
    // spawn 400 threads to enqueue elements
    for(int index = 0; index < 400; index++) {
        int *ptr = malloc(sizeof(int));
        *ptr = index;

        if(pthread_create(&thread_ids_3[index], NULL, thread_enqueue, ptr) != 0)
            exit(EXIT_FAILURE);
    }

    pthread_t thread_ids_4[600];
    // spawn 600 threads to dequeue elements
    for(int index = 0; index < 600; index++) {
        if(pthread_create(&thread_ids_4[index], NULL, thread_dequeue, NULL) != 0)
            exit(EXIT_FAILURE);
    }

    // wait for threads to die before checking queue
    for(int index = 0; index < 1000; index++) {
        pthread_join(thread_ids[index], NULL);
    }
    // wait for threads to die before checking queue
    for(int index = 0; index < 250; index++) {
        pthread_join(thread_ids_2[index], NULL);
    }
    // wait for threads to die before checking queue
    for(int index = 0; index < 400; index++) {
        pthread_join(thread_ids_3[index], NULL);
    }
    // wait for threads to die before checking queue
    for(int index = 0; index < 600; index++) {
        pthread_join(thread_ids_4[index], NULL);
    }

    // get number of items in queue
    int num_items;
    if(sem_getvalue(&global_queue->items, &num_items) != 0)
        exit(EXIT_FAILURE);

    cr_assert_eq(num_items, 550, "Had %d items. Expected: %d", num_items, 550);
}

