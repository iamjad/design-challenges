
CREATE DATABASE myshop;
use myshop;

create table if not exists myshop.item
(
    item_id        bigint auto_increment primary key,
    item_name      varchar(100) not null,
    item_type      varchar(50)  null,
    picked_time    timestamp    null,
    picked_by      bigint       null,
    purchased_time timestamp    null
);

/* This index on picked_time speed up the PickItem Stored Procedure performance. */
create index idx_picked_time on myshop.item (picked_time);

/* InsertItems Stored Procedure */
create
    definer = root@localhost procedure InsertItems()
BEGIN
    DECLARE counter INT DEFAULT 0;
    WHILE counter < 100000 DO
            INSERT INTO item (item_name, item_type)
            VALUES ('MacBook Pro', 'laptop');

            SET counter = counter + 1;
        END WHILE;
END;


/* PickItem Stored Procedure */
create
    definer = root@localhost procedure PickItem(IN p_user_id int, OUT pickedItemId int)
BEGIN
    DECLARE itemId INT;
    SELECT item_id INTO itemId
    FROM item
    WHERE picked_time IS NULL OR picked_time < TIMESTAMPADD(MINUTE, -10, CURRENT_TIMESTAMP())
    LIMIT 1
    FOR UPDATE SKIP LOCKED;

    IF itemId IS NOT NULL THEN
        UPDATE item
        SET picked_time = CURRENT_TIMESTAMP(),
            picked_by = p_user_id
        WHERE item_id = itemId;
        SET pickedItemId = itemId;
        COMMIT;
    ELSE
        ROLLBACK;
        SET pickedItemId = NULL;
    END IF;
END;


