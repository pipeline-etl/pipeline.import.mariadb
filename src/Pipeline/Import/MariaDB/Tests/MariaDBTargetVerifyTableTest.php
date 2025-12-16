<?php

/**
 * This file contains the MariaDBTargetVerifyTableTest class.
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
class MariaDBTargetVerifyTableTest extends MariaDBTargetTestCase
{

    /**
     * Test that verifyTable() throws an exception when the table is not set.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::verifyTable
     */
    public function testVerifyTableThrowsExceptionWhenTableNotSet(): void
    {
        $this->expectException('UnexpectedValueException');
        $this->expectExceptionMessage('No table defined to import to!');

        $method = $this->getReflectionMethod('verifyTable');

        $method->invoke($this->class);
    }

    /**
     * Test that verifyTable() does not throw an exception when the table is set.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::verifyTable
     */
    public function testVerifyTableWhenTableIsSet(): void
    {
        $this->setReflectionPropertyValue('table', 'foo');

        $method = $this->getReflectionMethod('verifyTable');

        $method->invoke($this->class);
    }

}

?>
