<?php

/**
 * This file contains the MariaDBTargetSetTargetTest class.
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
class MariaDBTargetSetTargetTest extends MariaDBTargetTestCase
{

    /**
     * Test that setTarget() sets the table name property.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::setTarget
     */
    public function testSetTargetSetsTableNameProperty(): void
    {
        $this->assertPropertyUnset('table');

        $this->class->setTarget('table');

        $this->assertPropertySame('table', 'table');
    }

}

?>
