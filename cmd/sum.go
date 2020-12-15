package cmd

import (
	"encoding/binary"
	"fmt"
	// go 键值对存储库
	bolt "github.com/etcd-io/bbolt"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/zhangguanzhang/google_containers/core"
	"go/types"
	"time"
)

func NewSumCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "sum",
		Short: "list all check sum",
		Args:  cobra.ExactArgs(1),
		Run:   listCheckSum,
	}
}

func listCheckSum(cmd *cobra.Command, args []string) {
	// 打开 boltdb 键值对文件
	db, err := bolt.Open(args[0], 0600, &bolt.Options{
		Timeout:  1 * time.Second,
		ReadOnly: true,
	})
	if err != nil {
		log.Fatalf("open the boltdb file %s error: %v", args[0], err)
	}
	defer db.Close()
	// 以只读事务方式执行传入 View() 的函数
	if err := db.View(func(tx *bolt.Tx) error {
		// ForEach(): 对根 Bucket 执行传入的函数,其中 Bucket 表示数据库中键值对的集合
		return tx.ForEach(func(bName []byte, b *bolt.Bucket) error {
			// 键值对集合的游标指针
			c := b.Cursor()
			// 对键值对进行遍历,并输出
			for k, v := c.First(); k != nil; k, v = c.Next() {
				// 值的长度为10
				if len(v) != int(types.Uint32) {
					fmt.Printf("wrong: bucket:%s key=%s\n", bName, k)
					continue
				}

				fmt.Printf("bucket:%-35s key=%-65s, value=%v\n", bName, k, binary.LittleEndian.Uint32(v))
			}
			return nil
		})
	}); err != nil {
		log.Fatal(err)
	}
}

func NewGetSumCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "gsum",
		Short: "get Sum",
		Args:  cobra.ExactArgs(1),
		Run:   getSum,
	}
}

// 获取镜像的 CheckSum
func getSum(cmd *cobra.Command, args []string) {
	for _, image := range args {
		// 获取镜像的 CheckSum
		crc32Value, err := core.GetManifestBodyCheckSum(image)
		if err != nil {
			log.Errorf("%s|%v", image, err)
		}
		fmt.Printf("%s | %d\n", image, crc32Value)
	}
}
