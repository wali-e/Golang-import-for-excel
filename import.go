package main

import (
	"database/sql"
	"flag"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/360EntSecGroup-Skylar/excelize"
	_ "github.com/go-sql-driver/mysql"
)

type Conn struct {
	drver,
	user,
	passw,
	host,
	port,
	dbname,
	charset string
	db *sql.DB
}

type Table struct {
	tableName string
	fields    []string
	pk        string
	data      []map[string]Row
	conn      *Conn
}

type Row struct {
	id      uint64
	name    string
	project string
	amount  uint64
	date    string
	info    string
}

// set conn
var conn = Conn{
	drver:   "mysql",
	user:    "root",
	passw:   "mysql@wali",
	host:    "127.0.0.1",
	port:    "3306",
	charset: "utf8",
	dbname:  "pwa",
}

// set db
var table = Table{
	tableName: "pwa_import",
	fields:    []string{"name", "project", "amount", "date", "info"},
	conn:      &conn,
}

// 拆分的文件切片
var fileSlice []string

// 表格栏
var CellCols = []string{"A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"}

// 切分长度
var ChunkSize = 1000

func (conn *Conn) connect() error {
	conf := conn.user + ":" + conn.passw + "@tcp(" + conn.host + ":" + conn.port + ")/" + conn.dbname
	if conn.charset != "" {
		conf = conf + "?charset=" + conn.charset
	}
	db, err := sql.Open(conn.drver, conf)

	if err != nil {
		fmt.Printf("错误:%s", err)
		return err
	}
	conn.db = db
	return nil
}

func (table *Table) insert(row Row) error {
	var err error
	field := strings.Join(table.fields, ",")
	sql := "insert into " + table.tableName + "(" + field + ") values("
	sql += "\"" + row.name + "\",\"" + row.project + "\",\"" + strconv.FormatUint(row.amount, 10) + "\",\"" + row.date + "\",\"" + row.info
	sql += "\");"
	res, err := table.conn.db.Query(sql)
	if err != nil {
		fmt.Printf("错误:%s", err)
		return err
	}
	defer res.Close()
	return err
}

func readExcl(file string, head bool) ([][]string, error) {
	f, err := excelize.OpenFile(file)
	if err != nil {
		return nil, err
	}
	// Get all the rows in the Sheet1.
	rows := f.GetRows("Sheet1")
	if !head {
		rows = rows[1:]
	}
	return rows, nil
}

func impt(file string, c chan int) {
	// read excel
	rows, err := readExcl(file, false)
	if err != nil {
		fmt.Println(err)
		c <- 0
		return
	}

	// connect db
	if conn.connect() != nil {
		fmt.Println(err)
		c <- 0
		return
	}

	// import
	count := 0
	for _, line := range rows {
		r := Row{}
		r.name = line[0]
		r.project = line[1]
		r.amount, _ = strconv.ParseUint(line[2], 10, 64)
		r.date = line[3]
		r.info = line[4]
		if err := table.insert(r); err != nil {
			fmt.Println(err.Error())
			c <- count
			return
		} else {
			count++
		}
	}
	c <- count
}

// excel 拆片
func ChunkExcel(file string) {
	f, err := excelize.OpenFile(file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	fmt.Println("---------- 开始生成拆分Excel ----------")

	var titles []string
	excelFiles := map[string]*excelize.File{}
	fileName := fmt.Sprintf("output%s%d-%d.xlsx", string(os.PathSeparator), 1, ChunkSize)
	excelFiles[fileName] = excelize.NewFile()
	firstSheet := excelFiles[fileName].NewSheet("Sheet1")
	excelFiles[fileName].SetActiveSheet(firstSheet)
	sheets := f.GetSheetMap()
	for _, sheetName := range sheets {
		rows := f.GetRows(sheetName)
		rowIndex := 2
		numIndex := 0
		for i, row := range rows {
			if i == 0 {
				//标题行
				for j, colCell := range row {
					titles = append(titles, colCell)
					//写入表头
					excelFiles[fileName].SetCellValue("Sheet1", fmt.Sprintf("%s1", CellCols[j]), colCell)
				}
			} else {
				//开始写入文件
				for j, colCell := range row {
					excelFiles[fileName].SetCellValue("Sheet1", fmt.Sprintf("%s%d", CellCols[j], rowIndex), colCell)
				}
				numIndex++
				if numIndex%ChunkSize == 0 {
					//保存
					fmt.Println(fmt.Sprintf("开始生成文件%s", path.Base(fileName)))
					if err := excelFiles[fileName].SaveAs(fileName); err != nil {
						fmt.Println(err)
						os.Exit(1)
					}
					//生成新文件
					fileName = fmt.Sprintf("output%s%d-%d.xlsx", string(os.PathSeparator), numIndex+1, numIndex+ChunkSize)
					excelFiles[fileName] = excelize.NewFile()
					firstSheet = excelFiles[fileName].NewSheet("Sheet1")
					excelFiles[fileName].SetActiveSheet(firstSheet)
					rowIndex = 1
					for j, colCell := range titles {
						//写入表头
						excelFiles[fileName].SetCellValue("Sheet1", fmt.Sprintf("%s1", CellCols[j]), colCell)
					}
				}

				rowIndex++
			}
		}
		//最后的保存
		fmt.Println(fmt.Sprintf("开始生成文件%s", path.Base(fileName)))
		if err := excelFiles[fileName].SaveAs(fileName); err != nil {
			fmt.Println(err)
			os.Exit(1)
		}
		break
	}

	fmt.Println("---------- 拆分结束 ----------")
}

// 获取拆分的excel
func getFilelist(path string) []string {
	files := []string{}
	err := filepath.Walk(path, func(path string, f os.FileInfo, err error) error {
		if f == nil {
			return err
		}
		if f.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		fmt.Printf("filepath.Walk() returned %v\n", err)
	}
	return files
}

func run() {
	i := len(fileSlice) //通道计数
	result := make(chan int)
	for _, file := range fileSlice {
		go impt(file, result)
	}

	count := 0
	for r := range result {
		count += r
		if i <= 1 {
			close(result)
		}
		i--
	}
	fmt.Println("成功写入：", count)
}

type sliceFlag []string

func (f *sliceFlag) String() string {
	return fmt.Sprintf("%v", []string(*f))
}

func (f *sliceFlag) Set(value string) error {
	*f = append(*f, value)
	return nil
}

func main() {
	var fields sliceFlag
	var excel string
	flag.StringVar(&conn.host, "h", "127.0.0.1", "数据库连接host")
	flag.StringVar(&conn.user, "u", "root", "数据库连接用户")
	flag.StringVar(&conn.passw, "pa", "123456", "数据库连接密码")
	flag.StringVar(&conn.port, "pr", "3306", "数据库连接密码")
	flag.StringVar(&conn.dbname, "db", "test", "数据库名")
	flag.StringVar(&conn.charset, "chr", "utf8", "数据库编码类型")
	flag.StringVar(&table.tableName, "tb", "test", "导入的目标表名")
	flag.IntVar(&ChunkSize, "size", 1000, "拆分长度")
	flag.StringVar(&excel, "exl", "./import.xlsx", "操作的Excel文件路径")
	flag.Var(&fields, "fls", "写入表字段 (default \"name,project,amount,date,info\")")
	flag.Parse()
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	if len(fields) != 0 {
		table.fields = fields
	}

	start := time.Now()
	ChunkExcel(excel)                 // 拆分文件
	fileSlice = getFilelist("output") // 获取拆分后的文件
	run()
	for _, file := range fileSlice {
		os.Remove(file)
	}
	fmt.Println("耗时：", time.Since(start))
}
