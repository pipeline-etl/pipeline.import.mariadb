<?php

/**
 * This file contains the MariaDBTargetSetBatchSizeTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetSetBatchSizeTest extends MariaDBTargetTestCase
{

    /**
     * Test that setBatchSize() sets the table name property.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::setBatchSize
     */
    public function testSetBatchSizeSetsProperty(): void
    {
        $this->assertPropertySame('batchSize', 50_000);

        $this->class->setBatchSize(5000);

        $this->assertPropertySame('batchSize', 5000);
    }

}

?>
