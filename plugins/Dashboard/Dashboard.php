<?php
/**
 * Piwik - Open source web analytics
 * 
 * @link http://piwik.org
 * @license http://www.gnu.org/licenses/gpl-3.0.html GPL v3 or later
 * @version $Id: Dashboard.php 6174 2012-04-07 02:30:49Z capedfuzz $
 * 
 * @category Piwik_Plugins
 * @package Piwik_Dashboard
 */

/**
 * @package Piwik_Dashboard
 */
class Piwik_Dashboard extends Piwik_Plugin
{
	public function getInformation()
	{
		return array(
			'description' => Piwik_Translate('Dashboard_PluginDescription'),
			'author' => 'Piwik',
			'author_homepage' => 'http://piwik.org/',
			'version' => Piwik_Version::VERSION,
		);
	}

	public function getListHooksRegistered()
	{
		return array( 
			'AssetManager.getJsFiles' => 'getJsFiles',
			'AssetManager.getCssFiles' => 'getCssFiles',
			'UsersManager.deleteUser' => 'deleteDashboardLayout',
			'Menu.add' => 'addMenus',
			'TopMenu.add' => 'addTopMenu',
		);
	}

	public static function getAllDashboards($login) {
		$dashboards = Piwik_FetchAll('SELECT iddashboard, name
									  FROM '.Piwik_Common::prefixTable('user_dashboard') .
									' WHERE login = ? ORDER BY iddashboard', array($login));
		$pos = 0;
		$nameless = 1;
		foreach ($dashboards AS &$dashboard) {
			if (!empty($dashboard['name'])) {
				$dashboard['name'] = Piwik_Common::unsanitizeInputValue($dashboard['name']);
			} else {
				$dashboard['name'] = Piwik_Translate('Dashboard_DashboardOf', $login);
				if($nameless > 1) {
					$dashboard['name'] .= " ($nameless)";
				}
				if(empty($dashboard['layout']))
				{
					$layout = '[]';
				}
				else
				{
					$layout = html_entity_decode($dashboard['layout']);
					$layout = str_replace("\\\"", "\"", $layout);
				}
				$dashboard['layout'] = Piwik_Common::json_decode($layout);
				$nameless++;
			}
			$pos++;
		}
		return $dashboards;
	}

	public function addMenus()
	{
		Piwik_AddMenu('Dashboard_Dashboard', '', array('module' => 'Dashboard', 'action' => 'embeddedIndex', 'idDashboard' => 1), true, 5);

		if (!Piwik::isUserIsAnonymous()) {
			$login = Piwik::getCurrentUserLogin();

			$dashboards = self::getAllDashboards($login);
			if (count($dashboards) > 1)
			{
				$pos = 0;
				foreach ($dashboards AS $dashboard) {
					Piwik_AddMenu('Dashboard_Dashboard', $dashboard['name'], array('module' => 'Dashboard', 'action' => 'embeddedIndex', 'idDashboard' => $dashboard['iddashboard']), true, $pos);
					$pos++;
				}
			}

		}
	}

	public function addTopMenu()
	{
		Piwik_AddTopMenu('General_Dashboard', array('module' => 'CoreHome', 'action' => 'index'), true, 1);
	}
	
	function getJsFiles( $notification )
	{
		$jsFiles = &$notification->getNotificationObject();
		
		$jsFiles[] = "plugins/Dashboard/templates/widgetMenu.js";
		$jsFiles[] = "libs/javascript/json2.js";
		$jsFiles[] = "plugins/Dashboard/templates/Dashboard.js";
		$jsFiles[] = "plugins/Dashboard/templates/dashboardWidget.js";
	}
	
	function getCssFiles( $notification )
	{
		$cssFiles = &$notification->getNotificationObject();
		
		$cssFiles[] = "plugins/CoreHome/templates/datatable.css";
		$cssFiles[] = "plugins/Dashboard/templates/dashboard.css";
	}

	function deleteDashboardLayout($notification)
	{
		$userLogin = $notification->getNotificationObject();
		Piwik_Query('DELETE FROM ' . Piwik_Common::prefixTable('user_dashboard') . ' WHERE login = ?', array($userLogin));
	}

	public function install()
	{
		// we catch the exception
		try{
			$sql = "CREATE TABLE ". Piwik_Common::prefixTable('user_dashboard')." (
					login VARCHAR( 100 ) NOT NULL ,
					iddashboard INT NOT NULL ,
					name VARCHAR( 100 ) NULL DEFAULT NULL ,
					layout TEXT NOT NULL,
					PRIMARY KEY ( login , iddashboard )
					)  DEFAULT CHARSET=utf8 " ;
			Piwik_Exec($sql);
		} catch(Exception $e){
			// mysql code error 1050:table already exists
			// see bug #153 http://dev.piwik.org/trac/ticket/153
			if(!Zend_Registry::get('db')->isErrNo($e, '1050'))
			{
				throw $e;
			}
		}
	}
	
	public function uninstall()
	{
		Piwik_DropTables(Piwik_Common::prefixTable('user_dashboard'));
	}
	
}