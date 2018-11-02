package db

import "github.com/globalsign/mgo"

var session *mgo.Session

func InitMongoDB(url string) *mgo.Session {

	var err error
	session, err = mgo.Dial(url)
	if err != nil {
		panic(err)
	}

	session.SetMode(mgo.Monotonic, true)

	return session
}

func GetSession() *mgo.Session {
	return session.Copy()
}
