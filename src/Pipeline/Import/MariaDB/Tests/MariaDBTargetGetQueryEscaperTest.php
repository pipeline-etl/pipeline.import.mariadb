<?php

/**
 * This file contains the MariaDBTargetGetQueryEscaperTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\MySQL\MySQLQueryEscaper;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDB\MariaDBTarget
 */
class MariaDBTargetGetQueryEscaperTest extends MariaDBTargetTestCase
{

    /**
     * Test that getQueryEscaper() returns a simple query builder.
     *
     * @covers \Pipeline\Import\MariaDB\MariaDBTarget::getQueryEscaper
     */
    public function testGetQueryEscaperReturnsSimpleQueryBuilder(): void
    {
        $this->assertInstanceOf(MySQLQueryEscaper::class, $this->class->getQueryEscaper());
    }

}

?>
