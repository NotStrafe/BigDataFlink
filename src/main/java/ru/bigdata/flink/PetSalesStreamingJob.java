package ru.bigdata.flink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Objects;

public class PetSalesStreamingJob {
    private static final DateTimeFormatter CSV_DATE = DateTimeFormatter.ofPattern("M/d/yyyy");
    private static final LocalDate UNKNOWN_DATE = LocalDate.of(1970, 1, 1);

    public static void main(String[] args) throws Exception {
        String bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092");
        String topic = env("KAFKA_TOPIC", "pet-sales");
        String groupId = env("KAFKA_GROUP_ID", "pet-sales-flink");
        String jdbcUrl = env("POSTGRES_JDBC_URL", "jdbc:postgresql://postgres:5432/pet_sales");
        String jdbcUser = env("POSTGRES_USER", "lab");
        String jdbcPassword = env("POSTGRES_PASSWORD", "lab");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.enableCheckpointing(10_000);

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId(groupId)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(jdbcUser)
                .withPassword(jdbcPassword)
                .build();

        JdbcExecutionOptions executionOptions = JdbcExecutionOptions.builder()
                .withBatchSize(250)
                .withBatchIntervalMs(1_000)
                .withMaxRetries(5)
                .build();

        DataStream<PetSaleEvent> events = executionEnvironment
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka pet-sales source")
                .map(new JsonToEvent())
                .name("Parse JSON messages")
                .filter(Objects::nonNull)
                .name("Drop invalid messages");

        events.map(PetSalesStreamingJob::toCustomer)
                .name("Build dim_customers")
                .addSink(customerSink(connectionOptions, executionOptions))
                .name("Write dim_customers");

        events.map(PetSalesStreamingJob::toSeller)
                .name("Build dim_sellers")
                .addSink(sellerSink(connectionOptions, executionOptions))
                .name("Write dim_sellers");

        events.map(PetSalesStreamingJob::toProduct)
                .name("Build dim_products")
                .addSink(productSink(connectionOptions, executionOptions))
                .name("Write dim_products");

        events.map(PetSalesStreamingJob::toStore)
                .name("Build dim_stores")
                .addSink(storeSink(connectionOptions, executionOptions))
                .name("Write dim_stores");

        events.map(PetSalesStreamingJob::toSupplier)
                .name("Build dim_suppliers")
                .addSink(supplierSink(connectionOptions, executionOptions))
                .name("Write dim_suppliers");

        events.map(PetSalesStreamingJob::toDate)
                .name("Build dim_dates")
                .addSink(dateSink(connectionOptions, executionOptions))
                .name("Write dim_dates");

        events.map(PetSalesStreamingJob::toFact)
                .name("Build fact_sales")
                .addSink(factSink(connectionOptions, executionOptions))
                .name("Write fact_sales");

        executionEnvironment.execute("Pet sales CSV stream to PostgreSQL star schema");
    }

    private static SinkFunction<DimCustomer> customerSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO dim_customers (
                    customer_key, source_file, source_customer_id, first_name, last_name, age, email,
                    country, postal_code, pet_type, pet_name, pet_breed
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (customer_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_customer_id = EXCLUDED.source_customer_id,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    age = EXCLUDED.age,
                    email = EXCLUDED.email,
                    country = EXCLUDED.country,
                    postal_code = EXCLUDED.postal_code,
                    pet_type = EXCLUDED.pet_type,
                    pet_name = EXCLUDED.pet_name,
                    pet_breed = EXCLUDED.pet_breed
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindCustomer, executionOptions, connectionOptions);
    }

    private static SinkFunction<DimSeller> sellerSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO dim_sellers (
                    seller_key, source_file, source_seller_id, first_name, last_name, email, country, postal_code
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (seller_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_seller_id = EXCLUDED.source_seller_id,
                    first_name = EXCLUDED.first_name,
                    last_name = EXCLUDED.last_name,
                    email = EXCLUDED.email,
                    country = EXCLUDED.country,
                    postal_code = EXCLUDED.postal_code
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindSeller, executionOptions, connectionOptions);
    }

    private static SinkFunction<DimProduct> productSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO dim_products (
                    product_key, source_file, source_product_id, name, category, price, stock_quantity,
                    pet_category, weight, color, size, brand, material, description, rating, reviews,
                    release_date, expiry_date
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (product_key) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_product_id = EXCLUDED.source_product_id,
                    name = EXCLUDED.name,
                    category = EXCLUDED.category,
                    price = EXCLUDED.price,
                    stock_quantity = EXCLUDED.stock_quantity,
                    pet_category = EXCLUDED.pet_category,
                    weight = EXCLUDED.weight,
                    color = EXCLUDED.color,
                    size = EXCLUDED.size,
                    brand = EXCLUDED.brand,
                    material = EXCLUDED.material,
                    description = EXCLUDED.description,
                    rating = EXCLUDED.rating,
                    reviews = EXCLUDED.reviews,
                    release_date = EXCLUDED.release_date,
                    expiry_date = EXCLUDED.expiry_date
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindProduct, executionOptions, connectionOptions);
    }

    private static SinkFunction<DimStore> storeSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO dim_stores (
                    store_key, name, location, city, state, country, phone, email
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (store_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    location = EXCLUDED.location,
                    city = EXCLUDED.city,
                    state = EXCLUDED.state,
                    country = EXCLUDED.country,
                    phone = EXCLUDED.phone,
                    email = EXCLUDED.email
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindStore, executionOptions, connectionOptions);
    }

    private static SinkFunction<DimSupplier> supplierSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO dim_suppliers (
                    supplier_key, name, contact, email, phone, address, city, country
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (supplier_key) DO UPDATE SET
                    name = EXCLUDED.name,
                    contact = EXCLUDED.contact,
                    email = EXCLUDED.email,
                    phone = EXCLUDED.phone,
                    address = EXCLUDED.address,
                    city = EXCLUDED.city,
                    country = EXCLUDED.country
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindSupplier, executionOptions, connectionOptions);
    }

    private static SinkFunction<DimDate> dateSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO dim_dates (
                    date_key, full_date, day, month, quarter, year
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (date_key) DO UPDATE SET
                    full_date = EXCLUDED.full_date,
                    day = EXCLUDED.day,
                    month = EXCLUDED.month,
                    quarter = EXCLUDED.quarter,
                    year = EXCLUDED.year
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindDate, executionOptions, connectionOptions);
    }

    private static SinkFunction<FactSale> factSink(
            JdbcConnectionOptions connectionOptions,
            JdbcExecutionOptions executionOptions
    ) {
        String sql = """
                INSERT INTO fact_sales (
                    sale_event_id, source_file, source_row_number, source_sale_id, customer_key, seller_key,
                    product_key, store_key, supplier_key, sale_date_key, sale_quantity, sale_total_price,
                    product_unit_price
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT (sale_event_id) DO UPDATE SET
                    source_file = EXCLUDED.source_file,
                    source_row_number = EXCLUDED.source_row_number,
                    source_sale_id = EXCLUDED.source_sale_id,
                    customer_key = EXCLUDED.customer_key,
                    seller_key = EXCLUDED.seller_key,
                    product_key = EXCLUDED.product_key,
                    store_key = EXCLUDED.store_key,
                    supplier_key = EXCLUDED.supplier_key,
                    sale_date_key = EXCLUDED.sale_date_key,
                    sale_quantity = EXCLUDED.sale_quantity,
                    sale_total_price = EXCLUDED.sale_total_price,
                    product_unit_price = EXCLUDED.product_unit_price
                """;
        return JdbcSink.sink(sql, PetSalesStreamingJob::bindFact, executionOptions, connectionOptions);
    }

    private static DimCustomer toCustomer(PetSaleEvent event) {
        DimCustomer row = new DimCustomer();
        row.customerKey = customerKey(event);
        row.sourceFile = event.sourceFile;
        row.sourceCustomerId = event.saleCustomerId;
        row.firstName = event.customerFirstName;
        row.lastName = event.customerLastName;
        row.age = event.customerAge;
        row.email = event.customerEmail;
        row.country = event.customerCountry;
        row.postalCode = event.customerPostalCode;
        row.petType = event.customerPetType;
        row.petName = event.customerPetName;
        row.petBreed = event.customerPetBreed;
        return row;
    }

    private static DimSeller toSeller(PetSaleEvent event) {
        DimSeller row = new DimSeller();
        row.sellerKey = sellerKey(event);
        row.sourceFile = event.sourceFile;
        row.sourceSellerId = event.saleSellerId;
        row.firstName = event.sellerFirstName;
        row.lastName = event.sellerLastName;
        row.email = event.sellerEmail;
        row.country = event.sellerCountry;
        row.postalCode = event.sellerPostalCode;
        return row;
    }

    private static DimProduct toProduct(PetSaleEvent event) {
        DimProduct row = new DimProduct();
        row.productKey = productKey(event);
        row.sourceFile = event.sourceFile;
        row.sourceProductId = event.saleProductId;
        row.name = event.productName;
        row.category = event.productCategory;
        row.price = event.productPrice;
        row.stockQuantity = event.productQuantity;
        row.petCategory = event.petCategory;
        row.weight = event.productWeight;
        row.color = event.productColor;
        row.size = event.productSize;
        row.brand = event.productBrand;
        row.material = event.productMaterial;
        row.description = event.productDescription;
        row.rating = event.productRating;
        row.reviews = event.productReviews;
        row.releaseDate = event.productReleaseDate;
        row.expiryDate = event.productExpiryDate;
        return row;
    }

    private static DimStore toStore(PetSaleEvent event) {
        DimStore row = new DimStore();
        row.storeKey = storeKey(event);
        row.name = event.storeName;
        row.location = event.storeLocation;
        row.city = event.storeCity;
        row.state = event.storeState;
        row.country = event.storeCountry;
        row.phone = event.storePhone;
        row.email = event.storeEmail;
        return row;
    }

    private static DimSupplier toSupplier(PetSaleEvent event) {
        DimSupplier row = new DimSupplier();
        row.supplierKey = supplierKey(event);
        row.name = event.supplierName;
        row.contact = event.supplierContact;
        row.email = event.supplierEmail;
        row.phone = event.supplierPhone;
        row.address = event.supplierAddress;
        row.city = event.supplierCity;
        row.country = event.supplierCountry;
        return row;
    }

    private static DimDate toDate(PetSaleEvent event) {
        LocalDate saleDate = effectiveSaleDate(event);
        DimDate row = new DimDate();
        row.dateKey = dateKey(saleDate);
        row.fullDate = saleDate;
        row.day = saleDate.getDayOfMonth();
        row.month = saleDate.getMonthValue();
        row.quarter = ((row.month - 1) / 3) + 1;
        row.year = saleDate.getYear();
        return row;
    }

    private static FactSale toFact(PetSaleEvent event) {
        FactSale row = new FactSale();
        row.saleEventId = event.sourceFile + ":row:" + event.sourceRowNumber;
        row.sourceFile = event.sourceFile;
        row.sourceRowNumber = event.sourceRowNumber;
        row.sourceSaleId = event.id;
        row.customerKey = customerKey(event);
        row.sellerKey = sellerKey(event);
        row.productKey = productKey(event);
        row.storeKey = storeKey(event);
        row.supplierKey = supplierKey(event);
        row.saleDateKey = dateKey(effectiveSaleDate(event));
        row.saleQuantity = event.saleQuantity;
        row.saleTotalPrice = event.saleTotalPrice;
        row.productUnitPrice = event.productPrice;
        return row;
    }

    private static void bindCustomer(PreparedStatement statement, DimCustomer row) throws SQLException {
        setText(statement, 1, row.customerKey);
        setText(statement, 2, row.sourceFile);
        setInteger(statement, 3, row.sourceCustomerId);
        setText(statement, 4, row.firstName);
        setText(statement, 5, row.lastName);
        setInteger(statement, 6, row.age);
        setText(statement, 7, row.email);
        setText(statement, 8, row.country);
        setText(statement, 9, row.postalCode);
        setText(statement, 10, row.petType);
        setText(statement, 11, row.petName);
        setText(statement, 12, row.petBreed);
    }

    private static void bindSeller(PreparedStatement statement, DimSeller row) throws SQLException {
        setText(statement, 1, row.sellerKey);
        setText(statement, 2, row.sourceFile);
        setInteger(statement, 3, row.sourceSellerId);
        setText(statement, 4, row.firstName);
        setText(statement, 5, row.lastName);
        setText(statement, 6, row.email);
        setText(statement, 7, row.country);
        setText(statement, 8, row.postalCode);
    }

    private static void bindProduct(PreparedStatement statement, DimProduct row) throws SQLException {
        setText(statement, 1, row.productKey);
        setText(statement, 2, row.sourceFile);
        setInteger(statement, 3, row.sourceProductId);
        setText(statement, 4, row.name);
        setText(statement, 5, row.category);
        setDecimal(statement, 6, row.price);
        setInteger(statement, 7, row.stockQuantity);
        setText(statement, 8, row.petCategory);
        setDecimal(statement, 9, row.weight);
        setText(statement, 10, row.color);
        setText(statement, 11, row.size);
        setText(statement, 12, row.brand);
        setText(statement, 13, row.material);
        setText(statement, 14, row.description);
        setDecimal(statement, 15, row.rating);
        setInteger(statement, 16, row.reviews);
        setDate(statement, 17, row.releaseDate);
        setDate(statement, 18, row.expiryDate);
    }

    private static void bindStore(PreparedStatement statement, DimStore row) throws SQLException {
        setText(statement, 1, row.storeKey);
        setText(statement, 2, row.name);
        setText(statement, 3, row.location);
        setText(statement, 4, row.city);
        setText(statement, 5, row.state);
        setText(statement, 6, row.country);
        setText(statement, 7, row.phone);
        setText(statement, 8, row.email);
    }

    private static void bindSupplier(PreparedStatement statement, DimSupplier row) throws SQLException {
        setText(statement, 1, row.supplierKey);
        setText(statement, 2, row.name);
        setText(statement, 3, row.contact);
        setText(statement, 4, row.email);
        setText(statement, 5, row.phone);
        setText(statement, 6, row.address);
        setText(statement, 7, row.city);
        setText(statement, 8, row.country);
    }

    private static void bindDate(PreparedStatement statement, DimDate row) throws SQLException {
        setInteger(statement, 1, row.dateKey);
        setDate(statement, 2, row.fullDate);
        setInteger(statement, 3, row.day);
        setInteger(statement, 4, row.month);
        setInteger(statement, 5, row.quarter);
        setInteger(statement, 6, row.year);
    }

    private static void bindFact(PreparedStatement statement, FactSale row) throws SQLException {
        setText(statement, 1, row.saleEventId);
        setText(statement, 2, row.sourceFile);
        setInteger(statement, 3, row.sourceRowNumber);
        setInteger(statement, 4, row.sourceSaleId);
        setText(statement, 5, row.customerKey);
        setText(statement, 6, row.sellerKey);
        setText(statement, 7, row.productKey);
        setText(statement, 8, row.storeKey);
        setText(statement, 9, row.supplierKey);
        setInteger(statement, 10, row.saleDateKey);
        setInteger(statement, 11, row.saleQuantity);
        setDecimal(statement, 12, row.saleTotalPrice);
        setDecimal(statement, 13, row.productUnitPrice);
    }

    private static String customerKey(PetSaleEvent event) {
        return sourceKey(event.sourceFile, "customer", event.saleCustomerId);
    }

    private static String sellerKey(PetSaleEvent event) {
        return sourceKey(event.sourceFile, "seller", event.saleSellerId);
    }

    private static String productKey(PetSaleEvent event) {
        return sourceKey(event.sourceFile, "product", event.saleProductId);
    }

    private static String storeKey(PetSaleEvent event) {
        return hashKey("store", event.storeName, event.storeLocation, event.storeCity, event.storeCountry, event.storeEmail);
    }

    private static String supplierKey(PetSaleEvent event) {
        return hashKey("supplier", event.supplierName, event.supplierEmail, event.supplierPhone, event.supplierAddress);
    }

    private static String sourceKey(String sourceFile, String entity, Integer sourceId) {
        return normalize(sourceFile) + ":" + entity + ":" + (sourceId == null ? "unknown" : sourceId);
    }

    private static String hashKey(String prefix, String... values) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String value : values) {
                digest.update(normalize(value).getBytes(StandardCharsets.UTF_8));
                digest.update((byte) 0);
            }
            byte[] bytes = digest.digest();
            StringBuilder hex = new StringBuilder(prefix).append(":");
            for (int i = 0; i < 16; i++) {
                hex.append(String.format("%02x", bytes[i]));
            }
            return hex.toString();
        } catch (Exception exception) {
            throw new IllegalStateException("Cannot build hash key", exception);
        }
    }

    private static int dateKey(LocalDate date) {
        return date.getYear() * 10_000 + date.getMonthValue() * 100 + date.getDayOfMonth();
    }

    private static LocalDate effectiveSaleDate(PetSaleEvent event) {
        return event.saleDate == null ? UNKNOWN_DATE : event.saleDate;
    }

    private static String normalize(String value) {
        return value == null || value.isBlank() ? "unknown" : value.trim();
    }

    private static void setText(PreparedStatement statement, int index, String value) throws SQLException {
        if (value == null || value.isBlank()) {
            statement.setNull(index, Types.VARCHAR);
        } else {
            statement.setString(index, value);
        }
    }

    private static void setInteger(PreparedStatement statement, int index, Integer value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.INTEGER);
        } else {
            statement.setInt(index, value);
        }
    }

    private static void setDecimal(PreparedStatement statement, int index, BigDecimal value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.NUMERIC);
        } else {
            statement.setBigDecimal(index, value);
        }
    }

    private static void setDate(PreparedStatement statement, int index, LocalDate value) throws SQLException {
        if (value == null) {
            statement.setNull(index, Types.DATE);
        } else {
            statement.setDate(index, Date.valueOf(value));
        }
    }

    private static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? defaultValue : value;
    }

    private static String text(JsonNode node, String name) {
        JsonNode value = node.get(name);
        if (value == null || value.isNull()) {
            return null;
        }
        String text = value.asText();
        return text == null || text.isBlank() ? null : text.trim();
    }

    private static Integer integer(JsonNode node, String name) {
        String value = text(node, name);
        if (value == null) {
            return null;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static BigDecimal decimal(JsonNode node, String name) {
        String value = text(node, name);
        if (value == null) {
            return null;
        }
        try {
            return new BigDecimal(value);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private static LocalDate localDate(JsonNode node, String name) {
        String value = text(node, name);
        if (value == null) {
            return null;
        }
        try {
            return LocalDate.parse(value, CSV_DATE);
        } catch (DateTimeParseException exception) {
            return null;
        }
    }

    public static class JsonToEvent extends RichMapFunction<String, PetSaleEvent> {
        private transient ObjectMapper objectMapper;

        @Override
        public void open(Configuration parameters) {
            this.objectMapper = new ObjectMapper();
        }

        @Override
        public PetSaleEvent map(String value) throws Exception {
            JsonNode node = objectMapper.readTree(value);
            PetSaleEvent event = new PetSaleEvent();
            event.sourceFile = normalize(text(node, "_source_file"));
            event.sourceRowNumber = integer(node, "_source_row_number");
            event.id = integer(node, "id");
            event.customerFirstName = text(node, "customer_first_name");
            event.customerLastName = text(node, "customer_last_name");
            event.customerAge = integer(node, "customer_age");
            event.customerEmail = text(node, "customer_email");
            event.customerCountry = text(node, "customer_country");
            event.customerPostalCode = text(node, "customer_postal_code");
            event.customerPetType = text(node, "customer_pet_type");
            event.customerPetName = text(node, "customer_pet_name");
            event.customerPetBreed = text(node, "customer_pet_breed");
            event.sellerFirstName = text(node, "seller_first_name");
            event.sellerLastName = text(node, "seller_last_name");
            event.sellerEmail = text(node, "seller_email");
            event.sellerCountry = text(node, "seller_country");
            event.sellerPostalCode = text(node, "seller_postal_code");
            event.productName = text(node, "product_name");
            event.productCategory = text(node, "product_category");
            event.productPrice = decimal(node, "product_price");
            event.productQuantity = integer(node, "product_quantity");
            event.saleDate = localDate(node, "sale_date");
            event.saleCustomerId = integer(node, "sale_customer_id");
            event.saleSellerId = integer(node, "sale_seller_id");
            event.saleProductId = integer(node, "sale_product_id");
            event.saleQuantity = integer(node, "sale_quantity");
            event.saleTotalPrice = decimal(node, "sale_total_price");
            event.storeName = text(node, "store_name");
            event.storeLocation = text(node, "store_location");
            event.storeCity = text(node, "store_city");
            event.storeState = text(node, "store_state");
            event.storeCountry = text(node, "store_country");
            event.storePhone = text(node, "store_phone");
            event.storeEmail = text(node, "store_email");
            event.petCategory = text(node, "pet_category");
            event.productWeight = decimal(node, "product_weight");
            event.productColor = text(node, "product_color");
            event.productSize = text(node, "product_size");
            event.productBrand = text(node, "product_brand");
            event.productMaterial = text(node, "product_material");
            event.productDescription = text(node, "product_description");
            event.productRating = decimal(node, "product_rating");
            event.productReviews = integer(node, "product_reviews");
            event.productReleaseDate = localDate(node, "product_release_date");
            event.productExpiryDate = localDate(node, "product_expiry_date");
            event.supplierName = text(node, "supplier_name");
            event.supplierContact = text(node, "supplier_contact");
            event.supplierEmail = text(node, "supplier_email");
            event.supplierPhone = text(node, "supplier_phone");
            event.supplierAddress = text(node, "supplier_address");
            event.supplierCity = text(node, "supplier_city");
            event.supplierCountry = text(node, "supplier_country");
            return event;
        }
    }

    public static class PetSaleEvent implements Serializable {
        public String sourceFile;
        public Integer sourceRowNumber;
        public Integer id;
        public String customerFirstName;
        public String customerLastName;
        public Integer customerAge;
        public String customerEmail;
        public String customerCountry;
        public String customerPostalCode;
        public String customerPetType;
        public String customerPetName;
        public String customerPetBreed;
        public String sellerFirstName;
        public String sellerLastName;
        public String sellerEmail;
        public String sellerCountry;
        public String sellerPostalCode;
        public String productName;
        public String productCategory;
        public BigDecimal productPrice;
        public Integer productQuantity;
        public LocalDate saleDate;
        public Integer saleCustomerId;
        public Integer saleSellerId;
        public Integer saleProductId;
        public Integer saleQuantity;
        public BigDecimal saleTotalPrice;
        public String storeName;
        public String storeLocation;
        public String storeCity;
        public String storeState;
        public String storeCountry;
        public String storePhone;
        public String storeEmail;
        public String petCategory;
        public BigDecimal productWeight;
        public String productColor;
        public String productSize;
        public String productBrand;
        public String productMaterial;
        public String productDescription;
        public BigDecimal productRating;
        public Integer productReviews;
        public LocalDate productReleaseDate;
        public LocalDate productExpiryDate;
        public String supplierName;
        public String supplierContact;
        public String supplierEmail;
        public String supplierPhone;
        public String supplierAddress;
        public String supplierCity;
        public String supplierCountry;
    }

    public static class DimCustomer implements Serializable {
        public String customerKey;
        public String sourceFile;
        public Integer sourceCustomerId;
        public String firstName;
        public String lastName;
        public Integer age;
        public String email;
        public String country;
        public String postalCode;
        public String petType;
        public String petName;
        public String petBreed;
    }

    public static class DimSeller implements Serializable {
        public String sellerKey;
        public String sourceFile;
        public Integer sourceSellerId;
        public String firstName;
        public String lastName;
        public String email;
        public String country;
        public String postalCode;
    }

    public static class DimProduct implements Serializable {
        public String productKey;
        public String sourceFile;
        public Integer sourceProductId;
        public String name;
        public String category;
        public BigDecimal price;
        public Integer stockQuantity;
        public String petCategory;
        public BigDecimal weight;
        public String color;
        public String size;
        public String brand;
        public String material;
        public String description;
        public BigDecimal rating;
        public Integer reviews;
        public LocalDate releaseDate;
        public LocalDate expiryDate;
    }

    public static class DimStore implements Serializable {
        public String storeKey;
        public String name;
        public String location;
        public String city;
        public String state;
        public String country;
        public String phone;
        public String email;
    }

    public static class DimSupplier implements Serializable {
        public String supplierKey;
        public String name;
        public String contact;
        public String email;
        public String phone;
        public String address;
        public String city;
        public String country;
    }

    public static class DimDate implements Serializable {
        public Integer dateKey;
        public LocalDate fullDate;
        public Integer day;
        public Integer month;
        public Integer quarter;
        public Integer year;
    }

    public static class FactSale implements Serializable {
        public String saleEventId;
        public String sourceFile;
        public Integer sourceRowNumber;
        public Integer sourceSaleId;
        public String customerKey;
        public String sellerKey;
        public String productKey;
        public String storeKey;
        public String supplierKey;
        public Integer saleDateKey;
        public Integer saleQuantity;
        public BigDecimal saleTotalPrice;
        public BigDecimal productUnitPrice;
    }
}
