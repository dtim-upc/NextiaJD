package edu.upc.essi.dtim.nextiajd.utils

import java.io.{File, FileOutputStream, IOException, InputStream}
import java.nio.file.{FileSystems, Files}
import java.util.zip.{ZipEntry, ZipInputStream}

object Unzip {
//  val INPUT_ZIP_FILE: String = "src/main/resources/my-zip.zip"
//  val OUTPUT_FOLDER: String = "src/main/resources/my-zip"

  def unZipIt(zipFile: InputStream): String = {

//    println(System.getProperty("java.io.tmpdir"))
    val outputFolder = Files.createTempDirectory("modelsFJ")
    val buffer = new Array[Byte](1024)

    try {

      // zip file content
      val zis: ZipInputStream = new ZipInputStream(zipFile)
      // get the zipped file list entry
      var ze: ZipEntry = zis.getNextEntry()

      while (ze != null) {
        val fileName = ze.getName()
        if (ze.isDirectory) {
          Files.createDirectories(FileSystems.getDefault().getPath(
            outputFolder + File.separator + fileName))
        } else {

          val newFile = new File(outputFolder + File.separator + fileName)

          // create folders
          new File(newFile.getParent()).mkdirs()
          val fos = new FileOutputStream(newFile)
          var len: Int = zis.read(buffer)
          while (len > 0) {
            fos.write(buffer, 0, len)
            len = zis.read(buffer)
          }
          fos.close()
        }
        ze = zis.getNextEntry()
      }

      zis.closeEntry()
      zis.close()
      outputFolder.toString

    } catch {
      case e: IOException =>
        // scalastyle:off println
        println("exception caught: " + e.getMessage)
        outputFolder.toString
      // scalastyle:on println
    }

  }
}
