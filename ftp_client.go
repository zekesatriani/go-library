package go_library

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/jlaffaye/ftp"
	"github.com/pkg/sftp"
	"golang.org/x/crypto/ssh"
)

// ----- >> Base Ftp Client
type FtpClient struct {
	Address  string `json:"host"`
	Port     int    `json:"port"`
	UserName string `json:"user_name"`
	Password string `json:"-"`
	Sftp     bool   `json:"sftp"`

	directory  string `json:"-"`
	inProgress bool   `json:"-"`

	ftpConn     *ftp.ServerConn `json:"-"`
	sshConn     *ssh.Client     `json:"-"`
	sftpClient  *sftp.Client    `json:"-"`
	isConnected bool            `json:"-"`
}

var mutex sync.RWMutex

// ----- >> Init Ftp Client
func NewFtpClient(address string, port int, userName string, password string, sftp ...bool) (*FtpClient, error) {
	res := &FtpClient{
		Address:  address,
		Port:     port,
		UserName: userName,
		Password: password,
		Sftp:     false,

		isConnected: false,
		inProgress:  false,
		directory:   ".",
	}

	if sftp != nil && len(sftp) > 0 {
		res.Sftp = sftp[0]
	}

	return res, res.Connect()
}

// ----- >> Connect FTP
func (d *FtpClient) Connect() error {
	var final_error error

	if d.Sftp {

		// --- SSH
		sshConnfig := &ssh.ClientConfig{
			User:            d.UserName,
			HostKeyCallback: ssh.InsecureIgnoreHostKey(),
			Auth: []ssh.AuthMethod{
				ssh.Password(d.Password),
			},
		}

		// Connect to the SSH server
		sshClient, err := ssh.Dial("tcp", d.Address+":"+fmt.Sprintf("%d", d.Port), sshConnfig)
		if err != nil {
			final_error = errors.New("Failed to connect to SSH server: " + err.Error())
		} else {
			d.sshConn = sshClient

			d.sftpClient, final_error = sftp.NewClient(d.sshConn)
			if final_error != nil {
				final_error = errors.New("Failed to open SFTP session: " + err.Error())
			} else {
				d.isConnected = true
			}
		}

	} else {

		// --- Ftp
		d.ftpConn, final_error = ftp.Dial(fmt.Sprintf("%s:%d", d.Address, d.Port))

		if final_error == nil {
			final_error = d.ftpConn.Login(d.UserName, d.Password)
			if final_error == nil {
				d.isConnected = true
				d.directory, _ = d.ftpConn.CurrentDir()
			}
		}

	}

	return final_error
}

// ----- >> Close FTP Connection
func (d *FtpClient) Quit() error {
	var final_error error

	if d.Sftp {

		// --- SSH
		err := d.sftpClient.Close()
		if err != nil {
			final_error = err
		}

		err = d.sshConn.Close()
		if err != nil && final_error == nil {
			final_error = err
		}

		d.isConnected = false

	} else {

		// --- Ftp
		final_error = d.ftpConn.Quit()
		d.isConnected = false

	}

	return final_error
}

func (d *FtpClient) IsConnected() bool {
	return d.isConnected
}

func (d *FtpClient) IsInProgress() bool {
	return d.inProgress
}

// ----- >> Get Current Dir
func (d *FtpClient) CurrentDir() (string, error) {
	if d.Sftp {

		// --- SSH
		return "", errors.New("failed: undefined process")

	} else {

		// --- Ftp
		return d.ftpConn.CurrentDir()

	}
}

// ----- >> Make Dir
func (d *FtpClient) MakeDir(path string) error {

	if !d.isConnected {
		err := d.Connect()
		if err != nil {
			return err
		}
	}

	if d.Sftp {

		// --- SSH
		return d.sftpClient.Mkdir(path)

	} else {

		// --- Ftp
		return d.ftpConn.MakeDir(path)
	}
}

// ----- >> Change FTP Directory
func (d *FtpClient) ChangeDir(directory string) error {
	var final_err error

	if !d.isConnected {
		err := d.Connect()
		if err != nil {
			return err
		}
	}

	if d.Sftp {

		// --- SSH

	} else {

		// --- Ftp
		if d.isConnected && d.directory != directory {
			var i int = 0
			for {
				directory, err := d.CurrentDir()
				if err != nil || directory == "/" || directory == "./" {
					break
				} else {
					d.ftpConn.ChangeDirToParent()
				}
				i++
			}

			final_err = d.ftpConn.ChangeDir(directory)
			if final_err == nil {
				d.directory = directory
			}
		}
	}

	return final_err
}

// ----- >> Get List Entries from FTP Directory
func (d *FtpClient) GetList(directory string) ([]*ftp.Entry, error) {
	var final_result []*ftp.Entry = []*ftp.Entry{}
	var final_err error

	if !d.isConnected {
		err := d.Connect()
		if err != nil {
			return nil, err
		}
	}

	if d.Sftp {

		// --- SSH
		res, err := d.sftpClient.ReadDir(directory)
		if err != nil {
			final_err = err
		} else {
			for _, val := range res {
				var entry_type ftp.EntryType = 0
				if val.IsDir() {
					entry_type = 1
				}
				final_result = append(final_result, &ftp.Entry{
					Name: val.Name(),
					Type: entry_type,
					Size: uint64(val.Size()),
					Time: val.ModTime(),
				})
			}
		}

		// return nil, errors.New("Failed: Undefined Process")

	} else {

		// --- Ftp
		err := d.ChangeDir(directory)

		if err != nil {
			final_err = err
		} else {

			entries, err := d.ftpConn.List(".")
			if err != nil {
				final_err = errors.New("Err FTP List >> " + err.Error())
			} else {
				final_result = entries
			}
		}
	}

	return final_result, final_err
}

func (d *FtpClient) GetStringFileType(t ftp.EntryType) string {
	switch t {
	case ftp.EntryTypeFile:
		return "file"
	case ftp.EntryTypeFolder:
		return "folder"
	case ftp.EntryTypeLink:
		return "link"
	default:
		return ""
	}
}

func (d *FtpClient) split_ftp_path(ftp_target_path string) (string, string) {
	var folder_path string = ""
	var filename string = ""

	// Check FTP Path
	arr_path := strings.Split(ftp_target_path, "/")
	for i, val := range arr_path {
		if i+1 == len(arr_path) {
			filename = val
		} else if val != "" && val != "." && val != ".." {
			folder_path += "/" + val
		}
	}

	return folder_path, filename
}

func (d *FtpClient) Delete(filePath string) error {
	var final_error error

	if d.Sftp {

		// --- SSH
		err := d.sftpClient.Remove(filePath)
		if err != nil && !strings.Contains(err.Error(), "No such file or directory") {
			return err
		}

	} else {

		// --- Ftp
		err := d.ftpConn.Delete(filePath)
		if err != nil && !strings.Contains(err.Error(), "No such file or directory") {
			return err
		}
	}

	return final_error
}

// ----- >> Upload to FTP Directory
func (d *FtpClient) Upload(local_source_path string, ftp_target_path string) error {
	var final_error error

	_ = d.Delete(ftp_target_path)

	mutex.Lock()
	d.inProgress = true
	mutex.Unlock()

	if d.Sftp {

		// --- SSH
		// Open an SFTP session over the SSH connection
		sftpClient, err := sftp.NewClient(d.sshConn)
		if err != nil {
			final_error = errors.New("failed to open sftp session: " + err.Error())
		}
		defer sftpClient.Close()

		// Read the local file
		var localFile *os.File
		if final_error == nil {
			localFilePath := local_source_path
			localFile, err = os.Open(localFilePath)
			if err != nil {
				final_error = errors.New("failed to open local file: " + err.Error())
			}
		}

		// Create the remote file path
		remoteFolderPath := filepath.Dir(ftp_target_path)
		remoteFileName := filepath.Base(ftp_target_path)
		remoteFilePath := strings.Replace(filepath.Join(remoteFolderPath, remoteFileName), `\`, `/`, -1)

		fmt.Println("remoteFolderPath : ", remoteFolderPath)
		fmt.Println("remoteFilePath : ", remoteFilePath)
		fmt.Println("remoteFileName : ", remoteFileName)

		// Create the remote file
		var remoteFile *sftp.File
		if final_error == nil {
			remoteFile, err = sftpClient.Create(remoteFilePath)
			if err != nil {
				final_error = errors.New("failed to create remote file: " + err.Error())
			}
			defer remoteFile.Close()
		}

		// Write the local file contents to the remote file
		var fileContents []byte
		if final_error == nil {
			fileContents, err = io.ReadAll(localFile)
			if err != nil {
				final_error = errors.New("failed to read local file contents: " + err.Error())
			}
		}

		if final_error == nil {
			_, err = remoteFile.Write(fileContents)
			if err != nil {
				final_error = errors.New("failed to write to remote file: " + err.Error())
			}
		}

		if localFile != nil {
			localFile.Close()
		}

	} else {

		// --- Ftp
		f, err := os.Open(local_source_path)
		if err != nil {
			final_error = err
		}

		if final_error == nil {
			err = d.ftpConn.Stor(ftp_target_path, f)
			if err != nil {
				final_error = err
			}
		}

		_ = f.Close()

	}

	mutex.Lock()
	d.inProgress = false
	mutex.Unlock()

	return final_error
}

// ----- >> Download file from FTP Directory
func (d *FtpClient) Download(ftp_source_path string, local_target_path string) error {
	var final_error error

	mutex.Lock()
	d.inProgress = true
	mutex.Unlock()

	if d.Sftp {

		// --- SSH
		// Open the remote file
		remoteFile, err := d.sftpClient.Open(ftp_source_path)
		if err != nil {
			return err
		}
		defer remoteFile.Close()

		// Create or truncate the local file
		localFile, err := os.Create(local_target_path)
		if err != nil {
			return err
		}
		defer localFile.Close()

		// Copy the contents from the remote file to the local file
		_, err = io.Copy(localFile, remoteFile)
		if err != nil {
			return err
		}

	} else {

		// --- Ftp
		folder_path, filename := d.split_ftp_path(ftp_source_path)
		if folder_path != "" {
			err := d.ChangeDir(folder_path)
			if err != nil {
				final_error = err
			}
		}

		if final_error == nil {

			fileData, err := d.ftpConn.Retr(filename)
			if err != nil {
				final_error = errors.New("Download File >> " + err.Error())
			} else {

				f, err := os.Create(local_target_path)
				if err != nil {
					final_error = errors.New("Download File >> " + err.Error())
				} else {
					_, err = io.Copy(f, fileData)
					if err != nil {
						final_error = errors.New("Download File >> " + err.Error())
					} else {
						fileData.Close()
						f.Close()
					}
				}
			}
		}

	}

	mutex.Lock()
	d.inProgress = false
	mutex.Unlock()

	return final_error
}
