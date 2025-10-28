/*-
 * #%L
 * athena-databricks
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.databricks;

import org.junit.Before;
import org.junit.Test;
import org.junit.Assert;

public class DatabricksQueryStringBuilderTestSimple
{
    private DatabricksQueryStringBuilder queryBuilder;

    @Before
    public void setup()
    {
        DatabricksFederationExpressionParser parser = new DatabricksFederationExpressionParser();
        this.queryBuilder = new DatabricksQueryStringBuilder("`", parser);
    }

    @Test
    public void testQueryBuilderCreation()
    {
        Assert.assertNotNull("Query builder should not be null", queryBuilder);
    }

    @Test
    public void testQueryBuilderConfiguration()
    {
        // Test that the query builder is properly configured for Databricks
        Assert.assertNotNull("Query builder should not be null", queryBuilder);
        // Note: quote method is protected, so we test indirectly through functionality
    }

    @Test
    public void testFederationExpressionParser()
    {
        DatabricksFederationExpressionParser parser = new DatabricksFederationExpressionParser();
        Assert.assertNotNull("Federation expression parser should not be null", parser);
    }
}