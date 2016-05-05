package main

import (
	"fmt"
	"github.com/jessevdk/go-flags"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"os"
	"strings"
	"time"
)

type cmdOpts struct {
	Host    string `short:"H" long:"host" default:"localhost" description:"Hostname"`
	Port    string `short:"p" long:"port" default:"3306" description:"Port"`
	User    string `short:"u" long:"user" default:"root" description:"Username"`
	Pass    string `short:"P" long:"password" default:"" description:"Password"`
	Channel string `short:"c" long:"channel" default:"" description:"Channel name for Multi source replication"`
}

func fetchSlaveStatus(db mysql.Conn, status map[string]string, channel string) error {
	query := fmt.Sprintf("SHOW SLAVE STATUS")
	if channel != "" {
		query = fmt.Sprintf("SHOW SLAVE STATUS FOR CHANNEL '%s'", db.Escape(channel))
	}
	rows, res, err := db.Query(query)
	if err != nil {
		return err
	}
	if len(rows) < 1 {
		return fmt.Errorf("No slave status retrieved")
	}

	for _, field := range res.Fields() {
		status[strings.ToLower(field.Name)] = rows[0].Str(res.Map(field.Name))
	}
	return nil
}

func waitMasterPosition(db mysql.Conn, master_log_file string, read_master_log_pos string, timeout int32, channel string) (int, error) {
	// SELECT MASTER_POS_WAIT('$file', $pos, $timeout, 'main-db')
	query := fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %s, %d)", master_log_file, read_master_log_pos, timeout)
	if channel != "" {
		query = fmt.Sprintf("SELECT MASTER_POS_WAIT('%s', %s, %d, '%s')", master_log_file, read_master_log_pos, timeout, db.Escape(channel))
	}
	rows, _, err := db.Query(query)
	if err != nil {
		return -1, err
	}
	if len(rows) < 1 || rows[0][0] == nil {
		return -1, fmt.Errorf("No result retrieved for master_pos_wait")
	}
	result := rows[0].Int(0)
	return result, nil
}

func main() {
	os.Exit(_main())
}

func _main() (st int) {
	st = 1
	opts := cmdOpts{}
	psr := flags.NewParser(&opts, flags.Default)
	_, err := psr.Parse()
	if err != nil {
		os.Exit(1)
	}

	db := mysql.New("tcp", "", fmt.Sprintf("%s:%s", opts.Host, opts.Port), opts.User, opts.Pass, "")
	err = db.Connect()
	if err != nil {
		fmt.Fprintf(os.Stderr, "couldn't connect DB: %s\n", err)
		return
	}
	defer db.Close()

	fmt.Fprintf(os.Stdout, "Wait slave catchup master..\n")
	for {
		status := make(map[string]string)
		err = fetchSlaveStatus(db, status, opts.Channel)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't fetch slave status: %s\n", err)
			return
		}
		if status["seconds_behind_master"] != "0" {
			time.Sleep(time.Duration(1) * time.Second)
			continue
		}
		break
	}

	fmt.Fprintf(os.Stdout, "Stop slave io_thread and Check master_post_wait\n")
	for {
		stop_query := fmt.Sprintf("STOP SLAVE IO_THREAD")
		start_query := fmt.Sprintf("START SLAVE IO_THREAD")
		if opts.Channel != "" {
			stop_query = fmt.Sprintf("STOP SLAVE IO_THREAD FOR CHANNEL '%s'", db.Escape(opts.Channel))
			start_query = fmt.Sprintf("START SLAVE IO_THREAD FOR CHANNEL '%s'", db.Escape(opts.Channel))
		}
		_, _, err := db.Query(stop_query)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Could not stop slave io_thread: %s", err)
			return
		}

		status := make(map[string]string)
		err = fetchSlaveStatus(db, status, opts.Channel)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Couldn't fetch slave status... restart slave io_thread: %s\n", err)
			_, _, _ = db.Query(start_query)
			return
		}

		fmt.Fprintf(os.Stdout, "master_pos_wait..\n")
		master_pos_wait, err := waitMasterPosition(db, status["master_log_file"], status["read_master_log_pos"], 10, opts.Channel)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed exec master_pos_wait... restart slave io_thread: %s\n", err)
			_, _, _ = db.Query(start_query)
			return
		}

		if master_pos_wait == -1 {
			fmt.Fprintf(os.Stderr, "master_pos_wait returns -1... restart slave io_thread: %s\n", err)
			_, _, _ = db.Query(start_query)
			time.Sleep(time.Duration(3) * time.Second)
			continue
		}

	}
	fmt.Fprintf(os.Stdout, "io_thread stopped safety. Do stop slave\n")
	query := fmt.Sprintf("STOP SLAVE")
	if opts.Channel != "" {
		query = fmt.Sprintf("STOP SLAVE FOR CHANNEL '%s'", db.Escape(opts.Channel))
	}
	_, _, err = db.Query(query)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not stop slave: %s", err)
		return
	}
	fmt.Fprintf(os.Stdout, "Slave just stopped safety\n")
	st = 0
	return
}
