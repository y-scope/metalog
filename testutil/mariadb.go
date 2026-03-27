package testutil
import ("database/sql";"testing")
type MariaDBContainer struct{DB *sql.DB}
func SetupMariaDB(t *testing.T)*MariaDBContainer{return nil}
func(c *MariaDBContainer)Teardown(t *testing.T){}
func(c *MariaDBContainer)LoadSchema(t *testing.T){}
