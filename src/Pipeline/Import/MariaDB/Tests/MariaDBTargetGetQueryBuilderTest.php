<?php

/**
 * This file contains the MariaDBTargetGetQueryBuilderTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\MySQL\MySQLDMLQueryBuilder;
use Lunr\Gravity\MySQL\MySQLSimpleDMLQueryBuilder;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetGetQueryBuilderTest extends MariaDBTargetTestCase
{

    /**
     * Test that getQueryBuilder() returns NULL if no query builder is active.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getQueryBuilder
     */
    public function testGetQueryBuilderWhenNoQueryBuilderActive(): void
    {
        $this->assertNull($this->class->getQueryBuilder());
    }

    /**
     * Test that getQueryBuilder() returns a simple query builder.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getQueryBuilder
     */
    public function testGetQueryBuilderReturnsSimpleQueryBuilder(): void
    {
        $this->setReflectionPropertyValue('builder', $this->realSimpleBuilder);

        $this->assertInstanceOf(MySQLSimpleDMLQueryBuilder::class, $this->class->getQueryBuilder());
    }

    /**
     * Test that getQueryBuilder() returns a normal query builder.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getQueryBuilder
     */
    public function testGetQueryBuilderReturnsQueryBuilder(): void
    {
        $this->setReflectionPropertyValue('builder', $this->realBuilder);

        $this->assertInstanceOf(MySQLDMLQueryBuilder::class, $this->class->getQueryBuilder());
    }

}

?>
