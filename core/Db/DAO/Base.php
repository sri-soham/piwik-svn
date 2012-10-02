<?php
/**
 * Piwik - Open source web analytics
 *
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 *
 * @category Piwik
 * @package Piwik
 */

/**
 * @package Piwik
 * @subpackage Piwik_Db
 */

abstract class Piwik_Db_DAO_Base
{
	protected $db;
	protected $table;

	public function __construct($db, $table)
	{
		$this->db = $db;
		$this->table = Piwik_Common::prefixTable($table);
	}

	public function getSqlRevenue($field)
	{
		return "ROUND(".$field.",".Piwik_Tracker_GoalManager::REVENUE_PRECISION.")";
	}

	public function getCount()
	{
		$sql = 'SELECT COUNT(*) FROM ' . $this->table;
		return $this->db->fetchOne($sql);
	}

	public function getDB()
	{
		return $this->db;
	}
}
