import java.sql.*;
import java.io.*;
public class zipcode {
    Connection conn;
    String URL = "jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0";


    public static void main(String[] args)  throws java.sql.SQLException{
        zipcode work = new zipcode();
        //work.openCSV();
    }

    public zipcode() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");

            String url = "jdbc:oracle:thin:@acadoradbprd01.dpu.depaul.edu:1521:ACADPRD0";
            this.URL = url;
            conn = DriverManager.getConnection(url, "criverac", "cdm1978229");
            System.out.println("Inserting values from CSV File");
            openCSV(conn);
            System.out.println("Insertion Completed");
            System.out.println("Now Joining Tables");
            selectTable(conn);
            System.out.println("Joining Completed");
            conn.close();

        }
        catch (ClassNotFoundException ex) {System.err.println("Class not found " + ex.getMessage());}
        catch (SQLException ex)           {System.err.println(ex.getMessage());}

    }

    private void selectTable(Connection conn) throws SQLException {
        String selectString = "SELECT R.name, Z.zip, Z.latitude, Z.longitude  FROM zipcode Z, restaurant_locations R WHERE Z.zip=R.zipcode";
        try {
           Statement selQuery = conn.createStatement();

           ResultSet rs = selQuery.executeQuery(selectString);

           while (rs.next()){
               String zip = String.valueOf(rs.getString("zip"));
               String name = rs.getString("name");
               String lat = String.valueOf(rs.getFloat("latitude"));
               String lon = String.valueOf(rs.getFloat("longitude"));
               String format = String.format("%s, %s, \"%s\", \"%s\"\n",name, zip,
                       lat, lon);
               System.out.print(format);
           }



        } catch (SQLException ex)
        {
            System.err.println("Select failure " + ex.getMessage());
        }

    }

    public void openCSV(Connection conn1){
        try {
            BufferedReader csvReader = new BufferedReader(new FileReader("/Users/Chad/Documents/DePaul/CSC453/HW4/ChIzipcode.csv"));
            String row;
            Statement st = conn1.createStatement();
            row = csvReader.readLine();
            if (row == null){
                System.out.println("Wrong CSV File");
            }
            while ((row = csvReader.readLine()) != null) {
                String[] data = row.split(",");
                for (int i =0; i < 7; i++){
                    data[i] = data[i].replace("\"", "");
                }
                int zip = Integer.parseInt(data[0]);
                String city = data[1];
                String state = data[2];
                Float lat = Float.parseFloat(data[3]);
                Float lon = Float.parseFloat(data[4]);
                int time = Integer.parseInt(data[5]);
                int dst = Integer.parseInt(data[6]);

                String format = String.format("INSERT INTO zipcode VALUES (%d, '%s', '%s', %f, %f, %d, %d)",zip, city,
                        state, lat, lon, time, dst);
                //System.out.println(format);
                st.executeUpdate(format);
            }
            } catch (SQLException throwables) {
            throwables.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}