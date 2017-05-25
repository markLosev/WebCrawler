package webcrawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

/**
 *
 * @author Mark
 */
public class EmailDataBase {
    private String connection;
    Connection dbConnection;
    ArrayList<String> dbEmails;
    
    public EmailDataBase() {
        dbEmails = new ArrayList<>();
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            System.out.println("Driver Successfully Loaded!");
            String driver = "jdbc:sqlserver:";
            String url = "//lcmdb.cbjmpwcdjfmq.us-east-1.rds.amazonaws.com:";
            String port = "1433";
            String username = "DS7";
            String password = "Touro123";
            String database = "DS7";
            connection = driver + url + port + ";databaseName=" + database + ";user=" + username + ";password=" + password + ";";
        } catch (ClassNotFoundException ex) {
            Logger.getLogger(EmailDataBase.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
    
    public void getEmails() throws SQLException {
         ResultSet rs = null;
         try {
             dbConnection = DriverManager.getConnection(connection);
                 
                System.out.println("Connected to Database!");
             try {
                PreparedStatement state = dbConnection.prepareStatement("SELECT * FROM Emails");
                System.out.println("Query Executed Successfully!");
                rs = state.executeQuery();
             } catch (SQLException ex) {
                 Logger.getLogger(EmailDataBase.class.getName()).log(Level.SEVERE, null, ex);
             }
        } catch (SQLException ex) {
                 Logger.getLogger(EmailDataBase.class.getName()).log(Level.SEVERE, null, ex);
             }
        extractEmails(rs);
    }

    private void extractEmails(ResultSet rs) throws SQLException {
        while(rs.next()) {
            dbEmails.add(rs.getString("Email"));
        }
    }
    
    public void checkAndUpdateEmails(ArrayList<String> emails) throws SQLException {
        getEmails();
        List<String> newEmails = emails.stream().filter((String i) -> !dbEmails.contains(i)).collect(Collectors.toList());
        if (!newEmails.isEmpty()) {
            updateEmails(newEmails);
        }
    }

    private void updateEmails(List<String> newEmails) {
        for (int i = 0; i < newEmails.size(); i++) {
            try {
                dbConnection = DriverManager.getConnection(connection);
                    System.out.println("Connected to Database!");
                    PreparedStatement state = dbConnection.prepareStatement ("INSERT INTO Emails" + "(Emails) VALUES" + "(?)");
                    state.setString(1, newEmails.get(i));
                   state.executeUpdate();
                   System.out.println("Query Executed Successfully!");
            } catch (SQLException ex) {
                 Logger.getLogger(EmailDataBase.class.getName()).log(Level.SEVERE, null, ex);
                 }
        }
    }
}
