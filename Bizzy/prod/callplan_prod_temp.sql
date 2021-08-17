CREATE TABLE callplan_prod.callplan(
    id BIGINT NOT NULL ,
    salesman_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    visit_id BIGINT NOT NULL,
    non_visit_id BIGINT NOT NULL,
    scheduled_at BIGINT,
    scheduled_date BIGINT NOT NULL,
    from_pattern VARCHAR(255) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT,
    soff_code VARCHAR(10) NOT NULL
) 

CREATE TABLE callplan_prod.daily_route(
    id VARCHAR(50) NOT NULL,
    routine_schedule_id VARCHAR(50) NOT NULL,
    salesman_id VARCHAR(30) NOT NULL,
    period BIGINT NOT NULL,
    customer_id VARCHAR(30) NOT NULL,
    workplace_id VARCHAR(30) NOT NULL,
    route_type VARCHAR(5) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT
)
  
CREATE TABLE callplan_prod.non_sales_reason(
    id BIGINT NOT NULL ,
    description VARCHAR(MAX) NOT NULL,
    active SMALLINT NOT NULL
)
        
CREATE TABLE callplan_prod.non_visit(
    id BIGINT NOT NULL,
    non_visit_reason_id SMALLINT NOT NULL,
    created_at BIGINT,
    updated_at BIGINT
)
        
CREATE TABLE callplan_prod.non_visit_reason(
    id SMALLINT NOT NULL ,
    description VARCHAR(255) NOT NULL
)
        
CREATE TABLE callplan_prod.out_route_log(
    id BIGINT NOT NULL,
    salesman_id VARCHAR(255) NOT NULL,
    customer_id VARCHAR(255) NOT NULL,
    activity_type VARCHAR(30) NOT NULL,
    reference_id VARCHAR(100) NOT NULL,
    created_latitude VARCHAR(100) NOT NULL,
    created_longitude VARCHAR(100) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT
)
        
CREATE TABLE callplan_prod.routine_schedule(
    id VARCHAR(255) NOT NULL,
    schedule_usage VARCHAR(255) NOT NULL,
    schedule_type VARCHAR(255) NOT NULL,
    include_sat SMALLINT NOT NULL,
    include_sun SMALLINT NOT NULL,
    description VARCHAR(255) NOT NULL,
    already_transfered SMALLINT NOT NULL,
    created_at BIGINT,
    updated_at BIGINT
)
        
CREATE TABLE callplan_prod.routine_schedule_item(
    routine_schedule_id VARCHAR(255) NOT NULL,
    sh_schedule_item INTEGER NOT NULL,
    dec_value NUMERIC(10,0) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT
)
        
CREATE TABLE callplan_prod.store_closed_image(
    id BIGINT NOT NULL,
    callplan_id BIGINT NOT NULL,
    url VARCHAR(255) NOT NULL,
    created_at BIGINT,
    updated_at BIGINT
)
        
CREATE TABLE callplan_prod.visit(
    id BIGINT NOT NULL,
    created_latitude VARCHAR(100) NOT NULL,
    created_longitude VARCHAR(100) NOT NULL,
    finished_latitude VARCHAR(100) NOT NULL,
    finished_longitude VARCHAR(100) NOT NULL,
    non_sales_reason_id SMALLINT NOT NULL,
    toko_closed_image_url VARCHAR(255) NOT NULL,
    finished_at BIGINT,
    created_at BIGINT,
    updated_at BIGINT
)
        
CREATE TABLE callplan_prod.visit_activity(
    id BIGINT NOT NULL ,
    callplan_id BIGINT NOT NULL,
    activity_type VARCHAR(30) NOT NULL,
    reference_id VARCHAR(100) NOT NULL
)