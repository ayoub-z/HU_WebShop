--DROP TABLES
ALTER TABLE "session" DROP CONSTRAINT FKsession585004;
ALTER TABLE buid DROP CONSTRAINT FKbuid933374;
ALTER TABLE "order" DROP CONSTRAINT FKorder25500;
ALTER TABLE viewed_before DROP CONSTRAINT FKviewed_bef518020;
ALTER TABLE viewed_before DROP CONSTRAINT FKviewed_bef890147;
ALTER TABLE previously_recommended DROP CONSTRAINT FKpreviously773426;
ALTER TABLE previously_recommended DROP CONSTRAINT FKpreviously826036;
ALTER TABLE product_order DROP CONSTRAINT FKproduct_or52075;
ALTER TABLE product_order DROP CONSTRAINT FKproduct_or544660;
DROP TABLE IF EXISTS buid CASCADE;
DROP TABLE IF EXISTS "order" CASCADE;
DROP TABLE IF EXISTS previously_recommended CASCADE;
DROP TABLE IF EXISTS product CASCADE;
DROP TABLE IF EXISTS product_order CASCADE;
DROP TABLE IF EXISTS profile CASCADE;
DROP TABLE IF EXISTS "session" CASCADE;
DROP TABLE IF EXISTS viewed_before CASCADE;



--CREATE TABLES
CREATE TABLE buid (
  _buid      varchar(255) NOT NULL, 
  profile_id varchar(255) NOT NULL, 
  PRIMARY KEY (_buid));
CREATE TABLE "order" (
  orderid    SERIAL NOT NULL, 
  session_id varchar(50) NOT NULL, 
  PRIMARY KEY (orderid));
CREATE TABLE previously_recommended (
  previously_recommended_id SERIAL NOT NULL,
  profile_id                varchar(255) NOT NULL,
  product_id                varchar(255) NOT NULL,
  PRIMARY KEY (previously_recommended_id));
CREATE TABLE product (
  _id                  varchar(255) NOT NULL,
  name                 varchar(255) NOT NULL,
  brand                varchar(255),
  category             varchar(255),
  description          varchar(10000),
  fast_mover           bool NOT NULL,
  herhaalaankopen      bool NOT NULL,
  selling_price        int4 NOT NULL,
  doelgroep            varchar(255),
  sub_category         varchar(255),
  sub_sub_category     varchar(255),
  sub_sub_sub_category varchar(255),
  PRIMARY KEY (_id));
CREATE TABLE product_order (
  product_order_id SERIAL NOT NULL,
  product_id       varchar(255) NOT NULL,
  orderorderid     int4 NOT NULL,
  PRIMARY KEY (product_order_id));
CREATE TABLE profile (
  _id        varchar(255) NOT NULL,
  ordercount int4,
  segment    varchar(255),
  PRIMARY KEY (_id));
CREATE TABLE "session" (
  _id       varchar(50) NOT NULL,
  buid_buid varchar(255) NOT NULL,
  has_sale  bool NOT NULL,
  PRIMARY KEY (_id));
CREATE TABLE viewed_before (
  viewed_before_id SERIAL NOT NULL,
  product_id       varchar(255) NOT NULL,
  profile_id       varchar(255) NOT NULL,
  PRIMARY KEY (viewed_before_id));
ALTER TABLE "session" ADD CONSTRAINT FKsession585004 FOREIGN KEY (buid_buid) REFERENCES buid (_buid);
ALTER TABLE buid ADD CONSTRAINT FKbuid933374 FOREIGN KEY (profile_id) REFERENCES profile (_id);
ALTER TABLE "order" ADD CONSTRAINT FKorder25500 FOREIGN KEY (session_id) REFERENCES "session" (_id);
ALTER TABLE viewed_before ADD CONSTRAINT FKviewed_bef518020 FOREIGN KEY (product_id) REFERENCES product (_id);
ALTER TABLE viewed_before ADD CONSTRAINT FKviewed_bef890147 FOREIGN KEY (profile_id) REFERENCES profile (_id);
ALTER TABLE previously_recommended ADD CONSTRAINT FKpreviously773426 FOREIGN KEY (profile_id) REFERENCES profile (_id);
ALTER TABLE previously_recommended ADD CONSTRAINT FKpreviously826036 FOREIGN KEY (product_id) REFERENCES product (_id);
ALTER TABLE product_order ADD CONSTRAINT FKproduct_or52075 FOREIGN KEY (product_id) REFERENCES product (_id);
ALTER TABLE product_order ADD CONSTRAINT FKproduct_or544660 FOREIGN KEY (orderorderid) REFERENCES "order" (orderid);

