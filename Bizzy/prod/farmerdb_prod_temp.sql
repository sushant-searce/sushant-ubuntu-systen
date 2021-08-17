
CREATE TABLE farmerdb_prod_temp.answer_type(
    id INTEGER NOT NULL ,
    answer_type VARCHAR(25) NOT NULL,
    description VARCHAR(255) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
    
CREATE TABLE farmerdb_prod_temp.cash_in_transit(
    id BIGINT NOT NULL,
    salesman_id BIGINT NOT NULL,
    branch_id BIGINT NOT NULL,
    customer_ship_to_id BIGINT NOT NULL,
    customer_sold_to_id BIGINT NOT NULL,
    bill_no BIGINT NOT NULL,
    bill_date BIGINT NOT NULL,
    due_date BIGINT NOT NULL,
    payment_date BIGINT,
    bill_amount BIGINT NOT NULL,
    amount_collected NUMERIC(18,4) NOT NULL,
    status VARCHAR(20) NOT NULL,
    reason VARCHAR(20) NOT NULL,
    cash_collected NUMERIC(18,4) NOT NULL,
    giro_collected NUMERIC(18,4) NOT NULL,
    transfer_collected NUMERIC(18,4) NOT NULL,
    url_image_giro VARCHAR(255) NOT NULL,
    url_image_transfer VARCHAR(255) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.customer_order(
    id BIGINT NOT NULL ,
    sales_id_sys VARCHAR(10) NOT NULL,
    cust_id BIGINT NOT NULL,
    customer_id_sys VARCHAR(10) NOT NULL,
    noo_id BIGINT NOT NULL,
    noo_local_id VARCHAR(100) NOT NULL,
    po_number VARCHAR(20) NOT NULL,
    so_number_sys VARCHAR(20) NOT NULL,
    status_code VARCHAR(4) NOT NULL,
    payment_type VARCHAR(10) NOT NULL,
    customer_name VARCHAR(40) NOT NULL,
    contact_name VARCHAR(40) NOT NULL,
    contact_phone VARCHAR(40) NOT NULL,
    shipping_address_full VARCHAR(200) NOT NULL,
    shipping_address VARCHAR(100) NOT NULL,
    shipping_rt_rw VARCHAR(7) NOT NULL,
    shipping_village VARCHAR(150) NOT NULL,
    shipping_district VARCHAR(150) NOT NULL,
    shipping_city VARCHAR(150) NOT NULL,
    shipping_province VARCHAR(150) NOT NULL,
    shipping_postal_code VARCHAR(8) NOT NULL,
    total_yay_discount NUMERIC(18,4) NOT NULL,
    total_bundle_discount NUMERIC(18,4) NOT NULL,
    total_cumulative_discount NUMERIC(18,4) NOT NULL,
    total_discount NUMERIC(18,4) NOT NULL,
    sub_total NUMERIC(18,4) NOT NULL,
    total NUMERIC(18,4) NOT NULL,
    distributor_code VARCHAR(5) NOT NULL,
    updated_by BIGINT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_by BIGINT NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.noo_address(
    id BIGINT NOT NULL ,
    noo_profile_id BIGINT NOT NULL,
    address VARCHAR(250) NOT NULL,
    province_id INTEGER NOT NULL,
    city_id_sys INTEGER NOT NULL,
    district_id_sys INTEGER NOT NULL,
    village_id_sys BIGINT NOT NULL,
    rt_rw VARCHAR(20) NOT NULL,
    postal_code INTEGER NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_by VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_by VARCHAR(50) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.noo_image(
    id BIGINT NOT NULL,
    noo_profile_id BIGINT NOT NULL,
    image_type VARCHAR(25) NOT NULL,
    image_url VARCHAR(MAX) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_by VARCHAR(25) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_by VARCHAR(25) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.noo_profile(
    id BIGINT NOT NULL ,
    noo_id BIGINT NOT NULL,
    cust_id BIGINT NOT NULL,
    outlet_name VARCHAR(100) NOT NULL,
    latitude VARCHAR(100) NOT NULL,
    longitude VARCHAR(100) NOT NULL,
    phone_number_sys VARCHAR(20) NOT NULL,
    ktp_name VARCHAR(100) NOT NULL,
    npwp_name VARCHAR(100) NOT NULL,
    ktp_no VARCHAR(50) NOT NULL,
    npwp_no VARCHAR(50) NOT NULL,
    customer_group_sys VARCHAR(25) NOT NULL,
    customer_group1_sys VARCHAR(25) NOT NULL,
    customer_group2_sys VARCHAR(25) NOT NULL,
    customer_group3_sys VARCHAR(25) NOT NULL,
    customer_group4_sys VARCHAR(25) NOT NULL,
    sales_office_sys VARCHAR(25) NOT NULL,
    sales_id_sys BIGINT NOT NULL,
    pkp_status VARCHAR(2) NOT NULL,
    payment_type SMALLINT NOT NULL,
    customer_order_id BIGINT NOT NULL,
    active VARCHAR(100) NOT NULL ,
    status VARCHAR(100) NOT NULL ,
    has_order SMALLINT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_by VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_by VARCHAR(50) NOT NULL,
    local_id VARCHAR(100) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.order_detail(
    id BIGINT NOT NULL ,
    order_id BIGINT NOT NULL,
    po_number VARCHAR(20) NOT NULL,
    so_number_sys VARCHAR(20) NOT NULL,
    so_item NUMERIC(18,0) NOT NULL,
    product_id BIGINT NOT NULL,
    product_id_sys VARCHAR(20) NOT NULL,
    combo_id_sys VARCHAR(20) NOT NULL,
    product_type VARCHAR(10) NOT NULL,
    quantity NUMERIC(18,4) NOT NULL,
    product_uom VARCHAR(4) NOT NULL,
    original_price NUMERIC(18,4) NOT NULL,
    sale_price NUMERIC(18,4) NOT NULL,
    yay_discount NUMERIC(18,4) NOT NULL,
    bundle_discount NUMERIC(18,4) NOT NULL,
    cumulative_discount NUMERIC(18,4) NOT NULL,
    discount NUMERIC(18,4) NOT NULL,
    tax_amount NUMERIC(18,4) NOT NULL,
    tax_rate NUMERIC(18,4) NOT NULL,
    sub_total NUMERIC(18,4) NOT NULL,
    total NUMERIC(18,4) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_by BIGINT NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_by BIGINT NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.order_process(
    id BIGINT NOT NULL ,
    sales_id_sys VARCHAR(10) NOT NULL,
    customer_name VARCHAR(40) NOT NULL,
    is_done SMALLINT NOT NULL,
    body VARCHAR(MAX) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT,
    body_hash VARCHAR(50) NOT NULL,
    local_id VARCHAR(100) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.prod_uom_detail(
    id INTEGER NOT NULL,
    product_code VARCHAR(25) NOT NULL,
    uom_code VARCHAR(3) NOT NULL,
    is_sale SMALLINT NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    created_by VARCHAR(50) NOT NULL,
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    updated_by VARCHAR(50) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.stock_taking(
    id INTEGER NOT NULL ,
    salesman_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL,
    created_at BIGINT,
    updated_at BIGINT,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.stock_taking_detail(
    id INTEGER NOT NULL,
    stock_taking_id INTEGER NOT NULL,
    product_code VARCHAR(20) NOT NULL,
    uom_code VARCHAR(10) NOT NULL,
    quantity INTEGER NOT NULL,
    created_at BIGINT,
    updated_at BIGINT,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.survey(
    id INTEGER NOT NULL,
    name VARCHAR(50) NOT NULL,
    description VARCHAR(MAX) NOT NULL,
    valid_from BIGINT,
    valid_to BIGINT,
    last_updated BIGINT,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.survey_answer(
    id INTEGER NOT NULL ,
    customer_id BIGINT NOT NULL,
    survey_id INTEGER NOT NULL,
    survey_question_id INTEGER NOT NULL,
    survey_answer VARCHAR(255) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
    
CREATE TABLE farmerdb_prod_temp.survey_question(
    id INTEGER NOT NULL ,
    survey_id INTEGER NOT NULL,
    question VARCHAR(500) NOT NULL,
    answer_type INTEGER NOT NULL,
    selection_type_id INTEGER NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)
        
CREATE TABLE farmerdb_prod_temp.survey_sales_office(
    survey_id INTEGER NOT NULL,
    soff_id INTEGER NOT NULL,
    soff_code VARCHAR(10) NOT NULL,
    __ts_ms BIGINT,
    __deleted VARCHAR(20)
)

        
