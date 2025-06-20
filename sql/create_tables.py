# noinspection SqlNoDataSourceInspection
CREATE_TABLES_SQL = [
    """
    CREATE TABLE DIM_CITIES
    (
        city_id             VARCHAR(50)    NOT NULL PRIMARY KEY,
        city_name           VARCHAR(100)   NOT NULL,
        state_code          CHAR(2)        NOT NULL,
        is_capital          BIT            NOT NULL,
        ibge_res_pop        INT            NULL,
        ibge_res_pop_bras   INT            NULL,
        ibge_res_pop_estr   INT            NULL,
        ibge_du             INT            NULL,
        ibge_du_urban       INT            NULL,
        ibge_du_rural       INT            NULL,
        ibge_pop            INT            NULL,
    );
    """,
    """
    CREATE TABLE DIM_PRODUCTS
    (
        product_id                    VARCHAR(50)    NOT NULL PRIMARY KEY,
        product_category_name         VARCHAR(255)   NULL,
        product_category_name_english VARCHAR(255)   NULL,
        product_name_length           INT            NULL,
        product_description_length    INT            NULL,
        product_photos_qty            INT            NULL,
        product_weight_g              INT            NULL,
        product_length_cm             INT            NULL,
        product_height_cm             INT            NULL,
        product_width_cm              INT            NULL
    );
    """,
    """
    CREATE TABLE DIM_TIMESTAMP
    (
        timestamp VARCHAR(10) NOT NULL PRIMARY KEY,
        [year]         INT            NOT NULL,
        [month]        INT            NOT NULL,
        [day]          INT            NOT NULL,
        [hour]         INT            NOT NULL
    );
    """,
    """
    CREATE TABLE DIM_REVIEWS
    (
        review_id               VARCHAR(50)    NOT NULL PRIMARY KEY,
        order_id                VARCHAR(50)    NOT NULL,
        review_score            int            NULL,
        review_comment_title_length    int   NOT NULL,
        review_comment_message_length  int   NOT NULL,
    );
    """,
    """
    CREATE TABLE FACT_ORDER_ITEMS
    (
        order_item_id              VARCHAR(50)    NOT NULL PRIMARY KEY,
        order_item_position        INT            NULL,
        order_id                   VARCHAR(50)    NOT NULL,
        product_id                 VARCHAR(50)    NOT NULL,
        review_id               VARCHAR(50)    NULL,
        seller_id                  VARCHAR(50)    NULL,
        seller_city_id             VARCHAR(50)    NULL,
        shipping_limit_timestamp   VARCHAR(10)       NULL,
        price                      DECIMAL(12,2)  NULL,
        freight_value              DECIMAL(12,2)  NULL,
        customer_unique_id                   VARCHAR(50)    NULL,
        customer_city_id                     VARCHAR(50)    NULL,
        order_status                         VARCHAR(50)    NULL,
        order_purchase_timestamp             VARCHAR(10)       NULL,
        order_approved_timestamp             VARCHAR(10)       NULL,
        order_delivered_carrier_timestamp    VARCHAR(10)       NULL,
        order_delivered_customer_timestamp   VARCHAR(10)       NULL,
        order_estimated_delivery_timestamp   VARCHAR(10)       NULL,

        CONSTRAINT FK_ORDERS_City
            FOREIGN KEY(customer_city_id) REFERENCES DIM_CITIES(city_id),
        CONSTRAINT FK_FACT_ORDERITEMS_Products
            FOREIGN KEY(product_id) REFERENCES DIM_PRODUCTS(product_id),
        CONSTRAINT FK_FACT_ORDERITEMS_Cities
            FOREIGN KEY(seller_city_id) REFERENCES DIM_CITIES(city_id),
        CONSTRAINT FK_FACT_ORDERITEMS_Reviews
            FOREIGN KEY(review_id) REFERENCES DIM_REVIEWS(review_id),
            
        -- timestamps
        CONSTRAINT FK_FACT_ORDERITEMS_Timestamp_Purchase
            FOREIGN KEY(order_purchase_timestamp) REFERENCES DIM_TIMESTAMP(timestamp),
        CONSTRAINT FK_FACT_ORDERITEMS_Timestamp_Approved
            FOREIGN KEY(order_approved_timestamp) REFERENCES DIM_TIMESTAMP(timestamp),
        CONSTRAINT FK_FACT_ORDERITEMS_Timestamp_Delivered_Carrier
            FOREIGN KEY(order_delivered_carrier_timestamp) REFERENCES DIM_TIMESTAMP(timestamp),
        CONSTRAINT FK_FACT_ORDERITEMS_Timestamp_Delivered_Customer
            FOREIGN KEY(order_delivered_customer_timestamp) REFERENCES DIM_TIMESTAMP(timestamp),
        CONSTRAINT FK_FACT_ORDERITEMS_Timestamp_Estimated_Delivery
            FOREIGN KEY(order_estimated_delivery_timestamp) REFERENCES DIM_TIMESTAMP(timestamp),
        CONSTRAINT FK_FACT_ORDERITEMS_Shipping_Limit
            FOREIGN KEY(shipping_limit_timestamp) REFERENCES DIM_TIMESTAMP(timestamp)
    );
    """
]
