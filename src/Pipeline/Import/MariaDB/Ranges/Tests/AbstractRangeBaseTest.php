<?php

/**
 * This file contains the AbstractRangeBaseTest class.
 *
 * SPDX-FileCopyrightText: Copyright 2026 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Ranges\Tests;

/**
 * This class contains tests for the AbstractRange class.
 *
 * @covers Pipeline\Import\MariaDB\Ranges\AbstractRange
 */
class AbstractRangeBaseTest extends AbstractRangeTestCase
{

    /**
     * Test that the target property is set correctly.
     */
    public function testTargetIsSet(): void
    {
        $this->assertPropertySame('target', $this->target);
    }

    /**
     * Test that the data property is an empty array by default.
     */
    public function testDataIsEmptyArray(): void
    {
        $this->assertArrayEmpty($this->getReflectionPropertyValue('data'));
    }

}

?>
