# Databricks Athena Connector Validation Checklist

## Pre-Deployment Validation
- [ ] JAR artifact built successfully
- [ ] JAR size within Lambda limits (< 250MB)
- [ ] All required dependencies included
- [ ] CloudFormation templates validated
- [ ] IAM permissions configured correctly

## Deployment Validation
- [ ] Lambda function deployed successfully
- [ ] Function handler configured correctly
- [ ] Environment variables set properly
- [ ] VPC configuration (if required) working
- [ ] Secrets Manager integration (if used) functional

## Connectivity Validation
- [ ] Basic connectivity test passes
- [ ] Authentication with Databricks successful
- [ ] SSL/TLS connection established
- [ ] Network connectivity from Lambda to Databricks verified

## Metadata Operations
- [ ] Schema discovery works correctly
- [ ] Table listing functions properly
- [ ] Column metadata retrieval accurate
- [ ] Data type mapping correct
- [ ] Partition information detected (if applicable)

## Data Retrieval Operations
- [ ] Basic SELECT queries execute successfully
- [ ] Predicate pushdown working (WHERE clauses)
- [ ] Column projection working (SELECT specific columns)
- [ ] Aggregation functions working
- [ ] JOIN operations with other data sources
- [ ] Complex queries with subqueries and CTEs

## Data Type Support
- [ ] String/VARCHAR types
- [ ] Numeric types (INT, BIGINT, DOUBLE, DECIMAL)
- [ ] Date and timestamp types
- [ ] Boolean types
- [ ] Array types
- [ ] Map types
- [ ] Struct types
- [ ] NULL value handling

## Performance Validation
- [ ] Query execution times acceptable
- [ ] Lambda memory usage within limits
- [ ] Lambda timeout not exceeded
- [ ] Predicate pushdown reducing data transfer
- [ ] Partition pruning working (if applicable)

## Error Handling
- [ ] Invalid schema names handled gracefully
- [ ] Invalid table names handled gracefully
- [ ] Invalid column names handled gracefully
- [ ] Connection failures handled properly
- [ ] Timeout scenarios handled correctly
- [ ] Data type conversion errors handled

## Edge Cases
- [ ] Empty result sets handled
- [ ] Large result sets handled
- [ ] Special characters in identifiers
- [ ] Reserved words as identifiers
- [ ] Case sensitivity handling
- [ ] Unicode data handling

## Monitoring and Logging
- [ ] CloudWatch logs generated correctly
- [ ] Error messages are informative
- [ ] Performance metrics available
- [ ] Debugging information sufficient

## Security Validation
- [ ] Credentials not exposed in logs
- [ ] Network traffic encrypted
- [ ] IAM permissions follow least privilege
- [ ] Secrets Manager integration secure (if used)

## Documentation and Maintenance
- [ ] Deployment documentation complete
- [ ] Configuration examples provided
- [ ] Troubleshooting guide available
- [ ] Performance tuning recommendations documented
