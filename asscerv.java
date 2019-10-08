import java.util.ArrayList;
import java.util.Scanner;
import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;


public class AsociacionCervecera {
	Connection conn;
	
	public static void main(String[] args) {
		
		AsociacionCervecera ac = new AsociacionCervecera();
		ac.DBconnect();
		ac.mainMenu();
		ac.DBclose();
	}
	
	public boolean DBconnect() {
		
		try {
		
		String drv = "com.mysql.jdbc.Driver";
        Class.forName(drv);
        
        String serverAddress = "localhost:3306";
        String db = "asociacioncervecera";
        String user = "bd";
        String pass = "bdupm";
        String url = "jdbc:mysql://" + serverAddress + "/" + db;
        conn = DriverManager.getConnection(url, user, pass);
        System.out.println("Conectado a la base de datos!");
        return true;
		} catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.err.println("Clase no encontrada. ¿Se ha cargado el driver en el proyecto?");
            System.err.println("URL: http://dev.mysql.com/downloads/connector/j/");
            return false;
	}
		catch (Exception e) {
			System.err.println("Error desconocido"+ e.getMessage());
			e.printStackTrace();
			return false;
		}
		
	}
	
	public boolean DBclose() {
		try {
		conn.close();
		} catch (Exception e){
			System.err.println("No se puede cerrar la conexion");
			return false;
		}
		
		return true;
	}
	
	public boolean createTableEmpleado() {
		
  try { Statement stmt = null;
		stmt = conn.createStatement();
		stmt.execute("CREATE TABLE `empleado`("
				+ "ID_empleado INT,"
				+ "nombre VARCHAR(50),"
				+ "direccion VARCHAR(100),"
				+ "telefono VARCHAR(15),"
				+ "salario FLOAT,"
				+ "ID_bar INT,"
				+ "PRIMARY KEY (ID_empleado),"
				+ "FOREIGN KEY (ID_bar) REFERENCES bar (ID_bar)"
				+ "ON DELETE CASCADE ON UPDATE CASCADE"
				+ ");");
		System.out.println("Table empleado created correctly!");
		stmt.close();
		return true;
	} catch (Exception ex) {
		System.err.println("Exception: " + ex.getMessage());
		return false;
	}
	}
	
	public boolean createTableGusta() {
		 try { Statement stmt = null;
			stmt = conn.createStatement();
			stmt.execute("CREATE TABLE `gusta`("
					+ "ID_socio INT,"
					+ "ID_bar INT,"
					+ "ID_cerveza INT,"
					+ "PRIMARY KEY (ID_socio, ID_cerveza, ID_bar),"
					+ "FOREIGN KEY (ID_socio) REFERENCES socio (ID_socio)"
					+ "ON DELETE CASCADE ON UPDATE CASCADE,"
					+ "FOREIGN KEY (ID_bar) REFERENCES bar (ID_bar)"
					+ "ON DELETE CASCADE ON UPDATE CASCADE,"
					+ "FOREIGN KEY (ID_cerveza) REFERENCES cerveza (ID_cerveza)"
					+"ON DELETE CASCADE ON UPDATE CASCADE"
					+ ");");
			System.out.println("Table gusta created correctly!");
			stmt.close();
			return true;
		} catch (Exception ex) {
			System.err.println("Exception: " + ex.getMessage());
			return false;
		}
	}
	
	public boolean loadEmpleados() {
		
		
		String query = "INSERT INTO empleado (ID_empleado, nombre, direccion, telefono, salario, ID_bar) VALUES (?,?,?,?,?,?);";
		String nombre[] = {"Carmen Martín", "Ana Ruiz", "Mario Moreno", "Laura Romero", "Luis Ruiz", "Benito Gil", "Dolores Molina", "Julio Garrido", "Pilar Romero"};
		String direccion[] = {"C/ Sol, 1", "C/ Luna, 2", "C/ Estrella, 3", "C/ Mercurio, 4", "C/ Venus, 5", "C/ Marte, 6", "C/ Júpiter, 7", "C/ Júpiter, 7", "C/ Saturno, 8"};
		String telefono[] = {"699999999", "699999988", "699999977", "699999966", "699999955", "699999944", "699999933", "699999922", "699999911"};
		Float salario[] = {(float)1600.00, (float)1300.00, (float)1200.00, (float)1450.00, (float)13500.00, (float)1500.00, (float)1350.00 , (float)1350.00, (float)1650.00 };
		int bar[] = {1, 2, 2, 3, 3, 3, 4, 4, 5};
		
		try {
		PreparedStatement pst = conn.prepareStatement(query);
			for(int i =0; i < 9; i++){
		pst.setInt(1, i+1);
		pst.setString(2,nombre[i]);
		pst.setString(3,direccion[i]);
		pst.setString(4,telefono[i]);
		pst.setFloat(5,salario[i]);
		pst.setInt(6,bar[i]);
		pst.executeUpdate();
		System.out.println("Añadido el"+i);
		}
		
		pst.close();
		} catch (Exception ex) {
			System.err.println("Exception: " + ex.getMessage());
			return false;
		}
		return true;
	}
	
	public boolean loadGustos(String fileName) {	
		
		try {
		conn.setAutoCommit(false);
		ArrayList<Gusto> array = readData("gustos.csv");
		String query = "INSERT INTO gusta (ID_socio, ID_cerveza, ID_bar) VALUES (?,?,?);";
		PreparedStatement pst = conn.prepareStatement(query);
		for(int i =0; i < array.size(); i++){
			pst.setInt(1, array.get(i).getIdSocio());
			pst.setInt(2, array.get(i).getIdCerveza());
			pst.setInt(3, array.get(i).getIdBar());
			pst.executeUpdate();
		}
		pst.close();
		conn.commit();
		return true;
		
	} catch(Exception e){
		System.err.println("Exception"+ e.getMessage());
		return false;
	}
		
	}
	
	public ArrayList<Bar> getBarData() {
		
		ArrayList<Bar> result = new ArrayList<>();
		try{
		Statement st = conn.createStatement();
		ResultSet rs = st.executeQuery("SELECT * from bar");
		int i= 0;
		while (rs.next()){
			result.add(i, new Bar(rs.getInt("ID_bar"), rs.getString("nombre"), rs.getString("direccion")));
			++i;
		}
		st.close();
		rs.close();
		return result;
		
		} catch (Exception e){
			System.err.println("Exception"+ e.getMessage());
			return null;
		}
		
	}
	
	
	public ArrayList<Cerveza> getCervezasFabricante() {
		ArrayList<String> resultf = new ArrayList<>();
		ArrayList<Cerveza> resultc = new ArrayList<>();
		Scanner sc = new Scanner(System.in);
		try {
			Statement stf = conn.createStatement();
			ResultSet rsf = stf.executeQuery("SELECT * from fabricante");
			int i = 0;
			while(rsf.next()){
			
			resultf.add(i,rsf.getString("nombre"));
			++i;
			
		}
		rsf.close();
		stf.close();
		if (resultf.isEmpty()){
			return null;
		}
		for (int j = 0; j<resultf.size(); j++){
			
			System.out.println(resultf.get(j));
		}
		String fab;
		do {
		System.out.println("Da un fabricante : \n");
		fab = sc.nextLine();
		}while (!(resultf.contains(fab))) ;
			
		
		Statement stc = conn.createStatement();
			ResultSet rsc = stc.executeQuery("SELECT cerveza.ID_cerveza, cerveza.nombre, cerveza.caracteristicas, cerveza.ID_fabricante"
					+ " FROM cerveza, fabricante  "
					+ "WHERE fabricante.nombre = '"+ fab+
					 "' AND fabricante.ID_fabricante = cerveza.ID_fabricante" );
			i = 0;
			while (rsc.next()){
				resultc.add(i, new Cerveza(rsc.getInt("ID_cerveza"), rsc.getString("nombre"), rsc.getString("caracteristicas"), rsc.getInt("ID_fabricante")));
				++i;
		}
			
			rsc.close();
			stc.close();
			return resultc;
			
		} catch (Exception e){
			System.err.println("Exception"+ e.getMessage());
			return null;
		}
	}
	
	
	public ArrayList<Cerveza> getCervezasPopulares() {
		ArrayList<Cerveza> result = new ArrayList<>();
		ArrayList<Cerveza> resultf = new ArrayList<Cerveza>();
		try {
			
		Statement stc = conn.createStatement();
		int numcervezas = 0;
		ResultSet rsc = stc.executeQuery("SELECT  cerveza.ID_cerveza,cerveza.nombre, cerveza.caracteristicas, cerveza.ID_fabricante, COUNT(ID_socio) "
				+ "FROM gusta g, cerveza cerveza "
				+ "WHERE g.ID_cerveza = cerveza.ID_cerveza "
				+ "GROUP BY ID_cerveza ORDER BY COUNT(ID_socio) DESC;");
		int i = 0;
		while (rsc.next()){
			result.add(i, new Cerveza(rsc.getInt("ID_cerveza"), rsc.getString("nombre"), rsc.getString("caracteristicas"), rsc.getInt("ID_fabricante")));
			++i;
			++numcervezas;
	}
		Statement sttc = conn.createStatement();
		ResultSet rstc = sttc.executeQuery("SELECT * from cerveza");
		sttc.close();
		rstc.close();
		if (numcervezas < 10){
			return new ArrayList<Cerveza>();
		} else {
			
			for (int k = 0; k< (int)0.1*numcervezas; k++){
			 resultf.add(result.get(k));
			}
			return resultf;
			
		}
			
		} catch(Exception e){
			System.err.println("Exception"+ e.getMessage());
			return null;
		}
		 
	}
	
	public boolean addFotoColumn() {
		try {
			Statement st = conn.createStatement();
			int rs = st.executeUpdate("ALTER TABLE empleado ADD COLUMN foto LONGBLOB");
			st.close();
			if(rs != 1){
				return true;
			} else return false;
		
		} catch(Exception e){
			System.err.println("Exception" + e.getMessage());
			return false;
		}
		
		
	
	}
	
	public boolean addEmpleadoFoto() {
		try {
			String query = "INSERT INTO empleado (ID_empleado, nombre, direccion, telefono, salario, ID_bar, foto) VALUES (?,?,?,?,?,?,?);";
			PreparedStatement pst = conn.prepareStatement(query);
			pst.setInt(1,10);
			pst.setString(2, "Homer Simpson");
			pst.setString(3, "742 Evergreen Terrace");
			pst.setString(4, null);
			pst.setFloat(5, (float)1500.00);
			pst.setInt(6, 1);
			File file  = new File("HomerSimpson.jpg");
			FileInputStream fis  = new FileInputStream(file);
			pst.setBinaryStream(7, fis, (int)file.length());
			pst.executeUpdate();
			System.out.println("Empleado añadido!");
			pst.close();
			return true;
		} catch (Exception e){
			System.err.println("Exception" + e.getMessage());
			return false;
		}
	}
	
	
	
	
	/*
	 *  The private section of the code starts here. It is convenient to take
	 *  a glance to the code, but it is STRICTLY FORBIDDEN to modify any
	 *  part of the code bellow these lines.
	 *  
	 *  Aquí comienza la parte "privada" del código. Es conviniente echar
	 *  un vistazo al código, pero está ESTRÍCTICAMENTE PROHIBIDO modificar
	 *  cualquier parte del código bajo estas líneas.
	 */
	
	private void mainMenu() {
		Scanner sc = new Scanner(System.in);
		char menuOption = 'a';
		
		// Main menu loop
		do {
			System.out.println("Escoja una opción: ");
			System.out.println("  1) Crear las tablas \"empleado\" y \"gusta\".");
			System.out.println("  2) Cargar datos de los empleados y los gustos.");
			System.out.println("  3) Listar los bares almacenados.");
			System.out.println("  4) Listar las cervezas de un fabricante.");
			System.out.println("  5) Listar las cervezas más populares.");
			System.out.println("  6) Añadir columna de foto a la tabla \"empleado\".");
			System.out.println("  7) Añadir un nuevo empleado con foto.");
			System.out.println("  0) Salir de la aplicación.");
			
			// Read user's option and check that it is a valid option
			menuOption = 'a';
			do {
				String line = sc.nextLine();
				if (line.length()==1) {
					menuOption = line.charAt(0);
				}
				if (menuOption<'0' || menuOption>'7') {
					System.out.println("Opción incorrecta.");
				}
			} while (menuOption<'0' || menuOption>'7');
			
			ArrayList<Cerveza> cervezas;
			Cerveza c = new Cerveza();
			ArrayList<Bar> bars = getBarData();
			Bar b = new Bar();
			
			// Call a specific method depending on the option
			switch (menuOption) {
				case '1':
					System.out.println("Creando tabla \"empleado\"...");
					createTableEmpleado();
					System.out.println("Creando tabla \"gusta\"...");
					createTableGusta();
					break;
				case '2':
					System.out.println("Cargando datos de la tabla \"empleado\"...");
					loadEmpleados();
					System.out.println("Cargando datos de la tabla \"gusta\"...");
					loadGustos("gustos.csv");
					break;
				case '3':
					bars = getBarData();
					System.out.println(b.barsToString(bars)+'\n');
					break;
				case '4':
					cervezas = getCervezasFabricante();
					System.out.println(c.cervezasToString(cervezas)+'\n');
					break;
				case '5':
					cervezas = getCervezasPopulares();
					System.out.println(c.cervezasToString(cervezas)+'\n');
					break;
				case '6':
					System.out.println("Añadiendo columa \"foto\" en la tabla \"empleado\"...");
					addFotoColumn();
					break;
				case '7':
					System.out.println("Añadiendo un nuevo empleado con foto...");
					addEmpleadoFoto();
					break;
			}
			
			if (menuOption!='0')
				System.out.println("¿Qué más desea hacer?");
			else
				System.out.println("¡Hasta pronto!");
		} while (menuOption!='0');
		
		sc.close();
	}
	
	private ArrayList<Gusto> readData(String fileName) {
		File f = new File(fileName);
		ArrayList<Gusto> result =  new ArrayList<Gusto>();
		
		try {
			Scanner sc_file = new Scanner(f);
			
			while(sc_file.hasNextLine()) {
				String[] fields = sc_file.nextLine().split(";");
				Gusto row = new Gusto(Integer.valueOf(fields[0]), Integer.valueOf(fields[1]), Integer.valueOf(fields[2]));
				result.add(row);
			}
			
			sc_file.close();
		} catch (Exception e) {
			System.err.println("Error al leer el fichero.");
		}
		
		return result;
	}
	
	
	static private class Bar {
		private int idBar;
		private String nombre;
		private String direccion;
		
		public Bar () {
			setIdBar(0);
			setNombre("");
			setDireccion("");
		}
		
		public Bar (int idBar, String nombre, String direccion) {
			setIdBar(idBar);
			setNombre(nombre);
			setDireccion(direccion);
		}

		public int getIdBar() {
			return idBar;
		}

		public void setIdBar(int idBar) {
			this.idBar = idBar;
		}

		public String getNombre() {
			return nombre;
		}

		public void setNombre(String nombre) {
			this.nombre = nombre;
		}

		public String getDireccion() {
			return direccion;
		}

		public void setDireccion(String direccion) {
			this.direccion = direccion;
		}
		
		public String barsToString(ArrayList<Bar> bars) {
			String result = "Listado de bares: \n";
			if (bars != null)
				for (Bar bar: bars) {
					result = result + "  " + bar.idBar + " - " + bar.nombre + " - " + bar.direccion + "\n";
				}
			return result;
		}
	}
	
	static private class Cerveza {
		private int idCerveza;
		private String nombre;
		private String caracteristicas;
		private int idFabricante;
		
		public Cerveza() {
			setIdCerveza(0);
			setNombre("");
			setCaracteristicas("");
			setIdFabricante(0);
		}
		
		public Cerveza(int id, String n, String c, int idFabricante) {
			setIdCerveza(id);
			setNombre(n);
			setCaracteristicas(c);
			setIdFabricante(idFabricante);
		}

		public int getIdCerveza() {
			return idCerveza;
		}

		public void setIdCerveza(int idCerveza) {
			this.idCerveza = idCerveza;
		}

		public String getNombre() {
			return nombre;
		}

		public void setNombre(String nombre) {
			this.nombre = nombre;
		}

		public String getCaracteristicas() {
			return caracteristicas;
		}

		public void setCaracteristicas(String caracteristicas) {
			this.caracteristicas = caracteristicas;
		}

		public int getIdFabricante() {
			return idFabricante;
		}

		public void setIdFabricante(int idFabricante) {
			this.idFabricante = idFabricante;
		}
		
		public String cervezasToString(ArrayList<Cerveza> cervezas) {
			String result = "Listado de cervezas: \n";
			if (cervezas != null)
				for (Cerveza c: cervezas) {
					result = result + "  " + c.idCerveza + " - " + c.nombre + " - " + c.caracteristicas + " - " + c.idFabricante + "\n";
				}
			return result;
		}
	}
	
	static private class Gusto {
		private int idSocio;
		private int idCerveza;
		private int idBar;
		
		public Gusto() {
			setIdSocio(0);
			setIdCerveza(0);
			setIdBar(0);
		}
		
		public Gusto(int socio, int cerveza, int bar) {
			setIdSocio(socio);
			setIdCerveza(cerveza);
			setIdBar(bar);
		}

		public int getIdSocio() {
			return idSocio;
		}

		public void setIdSocio(int socio) {
			idSocio = socio;
		}

		public int getIdCerveza() {
			return idCerveza;
		}

		public void setIdCerveza(int cerveza) {
			idCerveza = cerveza;
		}

		public int getIdBar() {
			return idBar;
		}

		public void setIdBar(int bar) {
			idBar = bar;
		}
	}

}