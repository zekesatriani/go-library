package libraries

import (
	"fmt"
	"github.com/jlaffaye/ftp"
	"os"
	"strings"
)

type FtpConnection struct {
	Address string
	Port string
	UserName string
	Password string
}

func (ftpConnection *FtpConnection) DeleteFile(filePath string) error {
	ftpConn, err := ftp.Dial(fmt.Sprintf("%s:%s", ftpConnection.Address, ftpConnection.Port))
	if err != nil {
		return err
	}

	err = ftpConn.Login(ftpConnection.UserName, ftpConnection.Password)
	if err != nil {
		return err
	}

	err = ftpConnection.deleteFile(ftpConn, filePath)
	if err != nil {
		return err
	}

	_ = ftpConn.Logout()

	return nil

}

func (ftpConnection *FtpConnection) deleteFile(ftpConn *ftp.ServerConn, filePath string) error {
	err := ftpConn.Delete(filePath)
	if err != nil && !strings.Contains(err.Error(), "No such file or directory") {
		return err
	}
	return nil
}


func (ftpConnection *FtpConnection) UploadFile(source string, destination string) error {
	ftpConn, err := ftp.Dial(fmt.Sprintf("%s:%s", ftpConnection.Address, ftpConnection.Port))
	if err != nil {
		return err
	}

	err = ftpConn.Login(ftpConnection.UserName, ftpConnection.Password)
	if err != nil {
		return err
	}

	err = ftpConnection.deleteFile(ftpConn, destination)
	if err != nil {
		return err
	}

	f, err := os.Open(source)
	if err != nil {
		return err
	}

	err = ftpConn.Stor(destination, f)
	if err != nil {
		return err
	}

	_ = f.Close()
	_ = ftpConn.Logout()

	return nil
}


