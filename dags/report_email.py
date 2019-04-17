import os
import pandas as pd
import tabulate
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from airflow.hooks.email_plugin import EmailHook

LOCAL_DIR = '/tmp/'


def main(**kwargs):

    # Retrieve account name from Xcom
    ti = kwargs["ti"]
    source = ti.xcom_pull(
        task_ids="report_init_task")

    campus_name = source["campus"]
    image = source["image"]

    # Gather the reports
    df_facebook = pd.read_csv(
        LOCAL_DIR + campus_name + '_facebook_data_calculated.csv')

    df_facebook_po = pd.read_csv(
        LOCAL_DIR + campus_name + '_facebook_data_po_calculated.csv')

    df_google_network = pd.read_csv(
        LOCAL_DIR + campus_name + '_google_spent_per_network.csv', header=-1)

    df_google_search = pd.read_csv(
        LOCAL_DIR + campus_name + '_google_spent_search.csv', header=-1)

    df_leads = pd.read_csv(
        LOCAL_DIR + campus_name + '_last_week_leads_calculated.csv',
        header=-1, index_col=False)

    # Get date
    today = datetime.today()
    tday = today.strftime("%m-%d-%Y")

    # Send email with the results
    email_config = EmailHook()

    # Create message
    message = MIMEMultipart("alternative")
    message["Subject"] = campus_name + " Report " + str(tday)
    message["From"] = email_config.sender
    message["To"] = email_config.receiver

    # Allow white space n tabulate
    tabulate.PRESERVE_WHITESPACE = True

    html = """
    <html>

    <head>
      <title> Facebook Report </title>
      <meta http-equiv='Content-Type' content='text/html; charset=UTF-8'>
      <meta name='viewport' content='width=device-width, initial-scale=1'>
      <style type='text/css'>
        # outlook a {
          padding: 0;
        }

        .ReadMsgBody {
          width: 100%;
        }

        .ExternalClass {
          width: 100%;
        }

        .ExternalClass * {
          line-height: 100%;
        }

        body {
          margin: 0;
          padding: 0;
          -webkit-text-size-adjust: 100%;
          -ms-text-size-adjust: 100%;
        }

        table,
        td {{
          border-collapse: collapse;
          border: border: 1px solid black;
          mso-table-lspace: 0pt;
          mso-table-rspace: 0pt;
        }}

        img {
          border: 0;
          height: auto;
          line-height: 100%;
          outline: none;
          text-decoration: none;
          -ms-interpolation-mode: bicubic;
        }

        p {
          display: block;
          margin: 13px 0;
        }
      </style>

      <style type='text/css'>
        @media only screen and (min-width:480px) {
          .mj-column-per-60 {
            width: 60% !important;
            max-width: 60%;
          }

          .mj-column-per-40 {
            width: 40% !important;
            max-width: 40%;
          }

          .mj-column-per-100 {
            width: 100% !important;
            max-width: 100%;
          }

          .mj-column-per-45 {
            width: 45% !important;
            max-width: 45%;
          }

          .mj-column-per-11 {
            width: 11% !important;
            max-width: 11%;
          }

          .mj-column-per-89 {
            width: 89% !important;
            max-width: 89%;
          }

          .mj-column-per-75 {
            width: 75% !important;
            max-width: 75%;
          }

          .mj-column-per-25 {
            width: 25% !important;
            max-width: 25%;
          }

          .mj-column-per-65 {
            width: 65% !important;
            max-width: 65%;
          }

          .mj-column-per-35 {
            width: 35% !important;
            max-width: 35%;
          }
        }
      </style>
      <style type='text/css'>
        @media only screen and (max-width:480px) {
          table.full-width-mobile {
            width: 100% !important;
          }

          td.full-width-mobile {
            width: auto !important;
          }
        }
      </style>
    </head>

    <body>
      <div style=''>
        <div style='Margin:0px auto;max-width:800px;'>
          <table align='center' border='0' cellpadding='0' cellspacing='0' role='presentation' style='width:100%;'>
            <tbody>
              <tr>
                <td style='direction:ltr;font-size:0px;padding:20px 0;text-align:center;vertical-align:top;'>
                  <div class='mj-column-per-60 outlook-group-fix' style='font-size:13px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;'>
                    <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='vertical-align:top;' width='100%'>
                    </table>
                  </div>
                  <div class='mj-column-per-40 outlook-group-fix' style='font-size:13px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;'>
                    <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='vertical-align:top;' width='100%'>
                    </table>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div style='Margin:0px auto;max-width:800px;'>
          <table align='center' border='0' cellpadding='0' cellspacing='0' role='presentation' style='width:100%;'>
            <tbody>
              <tr>
                <td style='direction:ltr;font-size:0px;padding:0px;text-align:center;vertical-align:top;'>

                  <div class='mj-column-per-100 outlook-group-fix' style='font-size:13px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;'>
                    <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='vertical-align:top;' width='100%'>
                      <tr>
                        <td align='center' style='font-size:0px;padding:10px 25px;word-break:break-word;'>
                          <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='border-collapse:collapse;border-spacing:0px;'>
                            <tbody>
                              <tr>
                                <td style='width:550px;'>

                                  <img height='auto' src=""" + str(image) + """ style='border:0;display:block;outline:none;text-decoration:none;height:auto;width:100%;'
                                    width='550' />

                                </td>
                              </tr>
                            </tbody>
                          </table>
                        </td>
                      </tr>
                    </table>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div style='Margin:0px auto;max-width:800px;'>
          <table align='center' border='0' cellpadding='0' cellspacing='0' role='presentation' style='width:100%;'>
            <tbody>
              <tr>
                <td style='direction:ltr;font-size:0px;padding:0px;text-align:center;vertical-align:top;'>
                  <div class='mj-column-per-100 outlook-group-fix' style='font-size:13px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;'>
                    <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='vertical-align:top;' width='100%'>
                      <tr>
                        <td align='center' style='font-size:0px;padding:10px 25px;word-break:break-word;'>
                          <div style='font-family:Roboto, Helvetica, sans-serif;font-size:16px;font-weight:300;line-height:24px;text-align:center;color:#616161;'> Your automated report is here! </div>
                        </td>
                      </tr>
                    </table>
                  </div>
                </td>
              </tr>
            </tbody>
          </table>
        </div>
        <div style='Margin:0px auto;max-width:800px;'>
          <table align='center' border='0' cellpadding='0' cellspacing='0' role='presentation' style='width:100%;'>
            <tbody>
              <tr>
                <td style='direction:ltr;font-size:0px;padding:0px;text-align:center;vertical-align:top;'>
                  <div class='mj-column-per-45 outlook-group-fix' style='font-size:13px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;'>
                    <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='vertical-align:top;' width='100%'>
                      <tr>
                        <td align='center' style='font-size:0px;padding:0px;word-break:break-word;'>
                          <div style='font-family:Roboto, Helvetica, sans-serif;font-size:18px;font-weight:500;line-height:24px;text-align:center;color:#616161;'>""" + campus_name + """ REPORT """ + str(tday) + """ </div>
                        </td>
                      </tr>
                      <tr>
                        <td style='font-size:0px;padding:10px 25px;word-break:break-word;'>
                          <p style='border-top:solid 2px #616161;font-size:1;margin:0px auto;width:100%;'> </p>
                        </td>
                      </tr>
                      <tr>
                        <td style='font-size:0px;padding:10px 25px;word-break:break-word;'>
                          <p style='border-top:solid 2px #616161;font-size:1;margin:0px auto;width:45%;'> </p>

                        </td>
                      </tr>
                    </table>
                  </div>

                </td>
              </tr>
            </tbody>
          </table>
        </div>

        <div style='Margin:0px auto;max-width:800px;'>
          <table align='center' border='0' cellpadding='0' cellspacing='0' role='presentation' style='width:100%;'>
            <tbody>
              <tr>
                <td style='direction:ltr;font-size:0px;padding:0px;padding-top:30px;text-align:center;vertical-align:top;'>

                  <div class='mj-column-per-100 outlook-group-fix' style='font-size:13px;text-align:left;direction:ltr;display:inline-block;vertical-align:top;width:100%;'>
                    <table border='0' cellpadding='0' cellspacing='0' role='presentation' style='vertical-align:top;' width='100%'>
                      <tr>
                        <td align='left' style='font-size:0px;padding:10px 25px;word-break:break-word;'>
                          <div style='font-family:Roboto, Helvetica, sans-serif;font-size:16px;font-weight:300;line-height:24px;text-align:left;color:#616161;'>
                          <br>
                            <h3 style='color:rgb(31, 78, 165, .8)'> Facebook </h3>
                            <hr style='border: 1.6px solid rgb(31, 78, 165, .8);'>
                            <p> Last week """ + campus_name + """  spent on Facebook: </p>
                              <br>

                              """ + tabulate.tabulate(
        df_facebook, stralign="left", tablefmt='html') + """

                              <br>
                              <p> Breakdown per PO: </p>
                              <br>
                              """ + tabulate.tabulate(
        df_facebook_po, stralign="left", tablefmt='html') + """
                              <br>
                              <br>
                              <h3 style='color:rgb(155, 20, 20, .8)'> Google </h3>
                              <hr style='border: 1.6px solid rgb(155, 20, 20, .8);'>
                              <p> Last week """ + campus_name + """ spent on Google per network: </p>
                              <br>
                              """ + tabulate.tabulate(
        df_google_network, headers="firstrow", stralign="left", tablefmt='html') + """
                              <br>
                              <br>
                              <p> Breakdown per type of campaign: </p>
                              <br>
                              """ + tabulate.tabulate(
        df_google_search, headers="firstrow", stralign="left", tablefmt='html') + """
                            <br>
                            <br>
                            <h3> Leads per programs: </h3>
                            <br>
         """ + tabulate.tabulate(
        df_leads, headers="firstrow", stralign="right", tablefmt='html') + """
                          </div>
                        </td>
                      </tr>
                    </table>
                  </div>

                </td>
              </tr>
            </tbody>
          </table>
        </div>
    """

    part1 = MIMEText(html, "html")

    message.attach(part1)

    email_config.send_message(message)

    # Clean up
    os.remove(LOCAL_DIR + campus_name + '_facebook_data_calculated.csv')

    os.remove(LOCAL_DIR + campus_name + '_facebook_data_po_calculated.csv')

    os.remove(LOCAL_DIR + campus_name + '_google_ads_data_cleaned.csv')

    os.remove(LOCAL_DIR + campus_name + '_google_spent_per_network.csv')

    os.remove(LOCAL_DIR + campus_name + '_google_spent_search.csv')

    os.remove(LOCAL_DIR + campus_name + '_last_week_leads_calculated.csv')


if __name__ == '__main__':
    main()
