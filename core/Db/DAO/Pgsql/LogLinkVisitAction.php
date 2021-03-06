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
class Piwik_Db_DAO_Pgsql_LogLinkVisitAction extends Piwik_Db_DAO_LogLinkVisitAction
{
	public function __construct($db, $table)
	{
		parent::__construct($db, $table);
	}

	public function record($idvisit, $idsite, $idvisitor, $server_time,
						$url, $name, $ref_url, $ref_name, $time_spent,
						$custom_variables
						)
	{
		list($sql, $bind) = $this->paramsRecord(
			$idvisit, $idsite, $idvisitor, $server_time,
			$url, $name, $ref_url, $ref_name, $time_spent,
			$custom_variables
		);

		$this->db->query($sql, $bind);

		return $this->db->lastInsertId($this->table.'_idlink_va');
	}

	public function fetchAll()
	{
		$generic = Piwik_Db_Factory::getGeneric();
		$generic->checkByteaOutput();
		$sql = 'SELECT *, idvisitor::text AS idvisitor_text FROM ' . $this->table;
		$rows = $this->db->fetchAll($sql);
		while (list($k, $row) = each($rows))
		{
			if (!empty($row['idvisitor']))
			{
				$rows[$k]['idvisitor'] = $generic->db2bin($row['idvisitor_text']);
			}
			unset($rows[$k]['idvisitor_text']);
		}
		reset($rows);

		return $rows;
	}
} 
