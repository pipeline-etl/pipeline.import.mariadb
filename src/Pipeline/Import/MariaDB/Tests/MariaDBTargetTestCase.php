<?php

/**
 * This file contains the MariaDBTargetTestCase class.
 *
 * SPDX-FileCopyrightText: Copyright 2025 Framna Netherlands B.V., Zwolle, The Netherlands
 * SPDX-License-Identifier: MIT
 */

namespace Pipeline\Import\MariaDB\Tests;

use Lunr\Gravity\MySQL\Tests\Helpers\MySQLDatabaseAccessObjectTestCase;
use Pipeline\Import\MariaDB\MariaDBTarget;

/**
 * This class contains the tests for the MariaDBTarget.
 *
 * @covers \Pipeline\Import\MariaDBTarget
 */
abstract class MariaDBTargetTestCase extends MySQLDatabaseAccessObjectTestCase
{

    /**
     * Class to test.
     * @var MariaDBTarget
     */
    protected MariaDBTarget $class;

    /**
     * Set up test data.
     */
    public function setUp(): void
    {
        parent::setUp();

        $this->class = new MariaDBTarget($this->db, $this->logger);
        $this->baseSetUp($this->class);
    }

    /**
     * Destructor.
     */
    public function tearDown(): void
    {
        parent::tearDown();
    }

}

?>
