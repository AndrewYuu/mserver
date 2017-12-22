#include "queue.h"
#include <errno.h>
#include "debug.h"

queue_t *create_queue(void){
    queue_t *queue = calloc(1, sizeof(queue_t));
    sem_init(&queue->items, 0, 0);
    if(pthread_mutex_init(&queue->lock, NULL) != 0){
        exit(1);
    }
    return queue;
}

bool invalidate_queue(queue_t *self, item_destructor_f destroy_function){
    if(!self || !destroy_function){
        errno = EINVAL;
        return false;
    }

    pthread_mutex_lock(&self->lock);
    //FREE THE REMAINING queue_nod INSTANCES.
    while(self->front != NULL){
        // call destroy function on item in node
        // free node
        destroy_function(self->front->item);
        queue_node_t *temp = self->front;
        self->front = self->front->next;
        free(temp);
    }

    self->invalid = 1;

    pthread_mutex_unlock(&self->lock);
    return true;
}

bool enqueue(queue_t *self, void *item){
    if(!self || !item || self->invalid){
        errno = EINVAL;
        return false;
    }

    //CURRENT THREAD TAKES OWNERSHIP OF THE MUTEX AND IS LOCKED FOR OTHER THREADS THAT ATTEMPT TO RUN THIS.
    //PREVENTS CONCURRENT MODIFICATION OF THE DATASTRUCTURE BY MULTIPLE THREADS.
    pthread_mutex_lock(&self->lock);

    queue_node_t *new_node = calloc(1, sizeof(queue_node_t));
    new_node->item = item;

    if(self->front == NULL){
        self->front = new_node;
    }
    else{
        self->rear->next = new_node;
    }
    self->rear = new_node;

    debug("front: %p\n", self->front);
    debug("front item: %d\n", *(int *)(self->front->item));
    debug("rear: %p\n", self->rear);
    debug("rear item: %d\n", *(int *)(self->rear->item));

    //INCREMENT / V() SEMAPHORE ITEM COUNT.
    //INDICATES TO THE CONSUMER THAT THERE IS AN ITEM IN THE DATASTRUCTURE
    sem_post(&self->items);

    //UNLOCK THE MUTEX SO THAT ANOTHER THREAD CAN TAKE OWNERSHIP AND RUN.
    pthread_mutex_unlock(&self->lock);

    return true;
}

void *dequeue(queue_t *self){
    if(self == NULL){
        errno = EINVAL;
        return NULL;
    }
    void *temp_item;

    //DECREMENT / P() SEMAPHORE ITEM COUNT.
    //IF THERE IS NO ITEMS, I.E: THE SEMAPHORE IS 0, THE THREAD WILL BE BLOCKED UNTIL ANOTHER THREAD
    //ADDS TO THE DATASTRUCTURE, INCREMENTS THE SEMAPHORE, WHICH WILL INDICATE THE ABILITY TO THEN REMOVE.
    sem_wait(&self->items);

    pthread_mutex_lock(&self->lock);

    // preserve front in a temp var
    // increment front
    // free the temp
    // put back lock
    // return preserved item
    debug("front before free: %p\n", self->front);
    queue_node_t *temp;
    if(self->front != NULL){
        temp = self->front;
        temp_item = temp->item;
        //IF THE FRONT EQUALS THE REAR, WHEN AFTER FREEING THE FRONT, THE REAR SHOULD ALSO BE MOVED TO NULL.
        if(self->front == self->rear){
            self->rear = self->rear->next;
        }
        self->front = self->front->next;
        free(temp);
    }
    debug("front after free: %p\n", self->front);
    debug("rear after free: %p\n", self->rear);

    // if(self->front != NULL){
    //     temp_item = temp->item;
    // }

    debug("Return: %p\n", temp_item);
    debug("Return item: %d\n", *(int *)(temp_item));

    pthread_mutex_unlock(&self->lock);
    return temp_item;
}
