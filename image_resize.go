package main

/*
export GOPATH=/home/forge/dev.balu.io/scripts/golang
 */
import(
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awsutil"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/aws/session"
	"bytes"
	"net/http"
	"os"

	"github.com/disintegration/imaging"
	"runtime"

	"github.com/streadway/amqp"
	"log"

	"fmt"
	"encoding/json"
	"strings"

	"github.com/joho/godotenv"
	"os/user"

	"crypto/tls"
	"gopkg.in/gomail.v2"
	"strconv"
)

type ResizeMessage struct {
	Src   string   `json:"src"`
	Dst   string   `json:"dst"`
	Sizes []string `json:"sizes"`
}

func main() {
	err := godotenv.Load()
	if err != nil {
		failOnError(err, "Error loading .env file")
	}

	conn, ch, q := getQueue()
	defer conn.Close()
	defer ch.Close()

	msgs, err := ch.Consume(
		q.Name, // queue name
		"",     // consumer
		true,   // autoAck - remove from queue after get message
		false,  // exclusive
		false,  // noLocal
		false,  // noWait
		nil,    // amqp.Table
	)
	failOnError(err, "Failed consume to queue")

	for msg := range msgs {
		res := ResizeMessage{}
		json.Unmarshal(msg.Body, &res)
		for _, size := range res.Sizes {
			go resizeAndCopy(res, size)
		}
	}
}

func resizeAndCopy(resizeMessage ResizeMessage, size string) {
	resizeImage(resizeMessage, size)
	copyToS3Storage(resizeMessage.Dst + size + ".jpg")
}

func resizeImage(resizeMessage ResizeMessage, size string) {
	fmt.Printf("dst: " + resizeMessage.Dst + size + ".jpg\nsrc: " + resizeMessage.Src + "\nsize: " + size + "\n\n")

	runtime.GOMAXPROCS(runtime.NumCPU())

	srcImage, err := imaging.Open(resizeMessage.Src)
	if err != nil {
		failOnError(err, "Failed load original image")
	}

	dstImage := imaging.Resize(srcImage, getWidthBySize(size), 0, imaging.Lanczos)
	err = imaging.Save(dstImage, resizeMessage.Dst + size + ".jpg")
	if err != nil {
		currentUser, _ := user.Current()
		failOnError(err, "Failed save resize image. user(" + currentUser.Gid + ", " + currentUser.HomeDir +
			", " + currentUser.Name + ", " + currentUser.Uid + ", " + currentUser.Username + ") ")
	}
}

func copyToS3Storage(dstImage string) {
	aws_access_key_id := os.Getenv("AWS_KEY")
	aws_secret_access_key := os.Getenv("AWS_SECRET")
	token := ""
	creds := credentials.NewStaticCredentials(aws_access_key_id, aws_secret_access_key, token)
	_, err := creds.Get()
	failOnError(err, fmt.Sprintf("bad credentials: %s", err))
	cfg := aws.NewConfig().WithRegion(os.Getenv("AWS_REGION")).WithCredentials(creds)
	svc := s3.New(session.New(), cfg)

	file, err := os.Open(dstImage)
	failOnError(err, fmt.Sprintf("err opening file: %s", err))
	defer file.Close()
	fileInfo, _ := file.Stat()
	size := fileInfo.Size()
	buffer := make([]byte, size)

	file.Read(buffer)
	fileBytes := bytes.NewReader(buffer)
	fileType := http.DetectContentType(buffer)
	path := dstImage[strings.Index(dstImage, "/media/"):]
	params := &s3.PutObjectInput{
		Bucket: aws.String(os.Getenv("AWS_BUCKET")),
		Key: aws.String(path),
		ACL: aws.String("public-read"),
		Body: fileBytes,
		ContentLength: aws.Int64(size),
		ContentType: aws.String(fileType),
	}
	resp, err := svc.PutObject(params)
	//_, err = svc.PutObject(params)
	failOnError(err, fmt.Sprintf("bad response: %s", err))
	fmt.Printf("response %s", awsutil.StringValue(resp))
}

func getQueue() (*amqp.Connection, *amqp.Channel, *amqp.Queue) {
	connectionString := "amqp://" + os.Getenv("AMQP_USERNAME") + ":" + os.Getenv("AMQP_PASSWORD") +
		"@" + os.Getenv("AMQP_HOST") + ":" + os.Getenv("AMQP_PORT")
	conn, err := amqp.Dial(connectionString)
	failOnError(err, fmt.Sprintf("Failed connect to rabbitmq '%s'", connectionString))
	ch, err := conn.Channel()
	failOnError(err, "Failed connect to channel")
	q, err := ch.QueueDeclare(
		"image_resize",
		true,  // durable
		false, // auto delete queue
		false, // exclusive
		false, // no wait
		nil,   // amqp.Table
	)
	failOnError(err, "Failed queue declare")
	return conn, ch, &q
}

func getWidthBySize(size string) (int) {
	var res int
	switch size {
	case "xl":
		res = 1920
	case "lg":
		res = 800
	case "md":
		res = 460
	case "sm":
		res = 380
	case "xs":
		res = 160
	}
	return res
}

func failOnError(err error, msg string) {
	if err != nil {
		f, errorOpenFile := os.OpenFile("storage/logs/go_lang.log", os.O_APPEND | os.O_CREATE | os.O_RDWR, 0666)
		if errorOpenFile != nil {
			fmt.Printf("error opening file: %v", errorOpenFile)
		}
		defer f.Close()

		log.SetOutput(f)
		log.Printf("%s: %s", msg, err)

		sendErrorEmail(fmt.Sprintf("%s: %s", msg, err))
	}
}

func sendErrorEmail(msg string) {
	m := gomail.NewMessage()
	m.SetHeader("From", os.Getenv("MAIL_FROM_ADDRESS"), os.Getenv("MAIL_FROM_NAME"))
	m.SetHeader("To", "iskakov_zhanat@mail.ru")
	m.SetHeader("Subject", "balu: go lang image resize error!")
	m.SetBody("text/html", "<div style='background-color: aqua;'>" + msg + "</div>")

	mailPort, err := strconv.Atoi(os.Getenv("MAIL_PORT"))
	if err != nil {
		panic(err)
	}
	d := gomail.NewDialer(os.Getenv("MAIL_HOST"), mailPort, os.Getenv("MAIL_USERNAME"), os.Getenv("MAIL_PASSWORD"))
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}
	if err := d.DialAndSend(m); err != nil {
		panic(err)
	}
}